# provisioning/graph_client.py
#
# Thin Graph API client for the JML provisioning layer.
#
# WHY THIS EXISTS:
#   The Microsoft Graph SDK is async and speaks in SDK model objects.
#   The JML pipeline is synchronous and needs simple dicts back.
#   This file sits between the two — it wraps the exact Graph calls
#   Phase 1 needs, runs async SDK calls synchronously, and converts
#   responses into plain dicts the provisioner can work with.
#
#   It also owns the two custom exception types (GraphClientError,
#   UserNotFoundError) so the provisioner never has to catch SDK-specific
#   exceptions or inspect raw HTTP status codes directly.
#
# WHAT IT COVERS:
#   User creation and lookup
#   Group membership check and assignment
#   RBAC role assignment check and creation
#
# IDEMPOTENCY:
#   Every write operation has a paired check operation (e.g.
#   check_group_membership → add_group_member). The provisioner calls
#   the check first so the pipeline is safe to retry from the beginning
#   without creating duplicate users, groups, or role assignments.
#
# AUTH:
#   Local dev  — ClientSecretCredential, reads from local.settings.json.
#   Production — swap build_graph_client() to DefaultAzureCredential
#                in Block G. Nothing else in this file changes.
#
# PERMISSIONS REQUIRED (app registration or Managed Identity):
#   User.ReadWrite.All
#   Group.ReadWrite.All
#   RoleManagement.ReadWrite.Directory

import asyncio
import logging
import os
from dataclasses import dataclass

from azure.identity import ClientSecretCredential, DefaultAzureCredential
from msgraph.graph_service_client import GraphServiceClient
from msgraph.generated.models.user import User
from msgraph.generated.models.password_profile import PasswordProfile
from msgraph.generated.models.reference_create import ReferenceCreate

logger = logging.getLogger(__name__)


class GraphClientError(Exception):
    """
    Base exception for all Graph API failures in this module.
    Wraps SDK-specific exceptions so the provisioner only needs to
    catch one type regardless of what the SDK throws internally.
    """
    pass


class UserNotFoundError(GraphClientError):
    """
    Raised by get_user() when a UPN does not exist in Entra ID.
    Kept separate from GraphClientError so check_upn_exists() in
    validation_gate.py can distinguish a clean 404 from a real error
    without parsing exception messages.
    """
    pass


def build_graph_client() -> GraphServiceClient:
    """
    Build and return an authenticated GraphServiceClient.

    This is the only place in the codebase that knows how Graph auth works.
    Keeping it here means switching from client secret to Managed Identity
    (Block G) only requires changing this function — nothing in JmlGraphClient
    or the provisioner needs to change.

    For local dev, credentials are read from environment variables set in
    local.settings.json:
        AZURE_TENANT_ID     — Entra ID tenant ID
        AZURE_CLIENT_ID     — app registration client ID
        AZURE_CLIENT_SECRET — app registration client secret

    Credentials are never stored after the credential object is built —
    the SDK handles token acquisition and refresh from that point on.
    """
    tenant_id     = os.environ.get("AZURE_TENANT_ID", "")
    client_id     = os.environ.get("AZURE_CLIENT_ID", "")
    client_secret = os.environ.get("AZURE_CLIENT_SECRET", "")

    if not all([tenant_id, client_id, client_secret]):
        raise GraphClientError(
            "Missing Graph API credentials. Ensure AZURE_TENANT_ID, "
            "AZURE_CLIENT_ID, and AZURE_CLIENT_SECRET are set in "
            "local.settings.json."
        )

    credential = ClientSecretCredential(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret
    )

    return GraphServiceClient(credentials=credential)


class JmlGraphClient:
    """
    JML-scoped wrapper around GraphServiceClient.

    The Graph SDK is async. This class runs every SDK call synchronously
    via _run() so the provisioner stays simple and linear — no async/await
    anywhere outside this class.

    All methods raise GraphClientError (or its subclass UserNotFoundError)
    on failure. The provisioner catches these and records which step failed
    without needing to know anything about the underlying SDK.

    Construct via:
        client = JmlGraphClient(build_graph_client())
    """

    def __init__(self, graph_client: GraphServiceClient) -> None:
        self._client = graph_client

    def _run(self, coroutine):
        """
        Run an async Graph SDK coroutine synchronously.

        Tries the existing event loop first. If there isn't one (common in
        a plain Python script or a fresh thread), creates a new one and
        cleans it up after the call completes.
        """
        try:
            return asyncio.get_event_loop().run_until_complete(coroutine)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(coroutine)
            finally:
                loop.close()


    def get_user(self, upn: str) -> dict:
        """
        Retrieve a user from Entra ID by UPN.

        Returns a dict with id, upn, display_name, and account_enabled.

        Raises UserNotFoundError if the UPN does not exist — callers use
        this to distinguish a missing user from a real API error.
        Raises GraphClientError on any other failure.

        Called by check_upn_exists() (validation_gate.py) before provisioning
        and by the provisioner itself to detect retry scenarios where the user
        was created but the object ID was never recorded.
        """
        try:
            user = self._run(self._client.users.by_user_id(upn).get())

            if user is None:
                raise UserNotFoundError(f"User not found: {upn}")

            return {
                "id":              user.id,
                "upn":             user.user_principal_name,
                "display_name":    user.display_name,
                "account_enabled": user.account_enabled,
            }

        except UserNotFoundError:
            raise
        except Exception as e:
            if "not found" in str(e).lower() or "404" in str(e):
                raise UserNotFoundError(f"User not found: {upn}")
            raise GraphClientError(f"get_user failed for {upn}: {e}")

    def create_user(self, payload) -> dict:
        """
        Create a new Entra ID user from a canonical IdentityPayload.

        Returns a dict with the new user's Entra object id and upn.
        Group and role assignments are handled separately by the provisioner —
        this method only creates the user object.

        usage_location is hardcoded to "GB" because Entra requires it before
        a Microsoft 365 license can be assigned. Update this if the tenant
        spans multiple regions.

        The temporary password is deterministic (derived from employee_id)
        so a retry after a crash produces the same value — no confusion if
        the user was created but the pipeline didn't finish recording it.
        Must-change-on-first-login is enforced, so this value is never
        long-lived.
        """
        try:
            temp_password = _generate_temp_password(payload.employee_id)

            user                     = User()
            user.display_name        = payload.display_name
            user.user_principal_name = payload.upn
            user.mail_nickname       = payload.upn.split("@")[0]
            user.account_enabled     = True
            user.job_title           = payload.job_title
            user.department          = payload.department
            user.employee_id         = payload.employee_id
            user.usage_location      = "GB"  # Required for M365 license assignment

            password_profile                                  = PasswordProfile()
            password_profile.password                         = temp_password
            password_profile.force_change_password_next_sign_in = True
            user.password_profile                             = password_profile

            created = self._run(self._client.users.post(user))

            # Guard against a silent Graph API failure — the SDK can return None
            # instead of raising if the call technically succeeded but returned
            # no object. Raising here prevents an AttributeError on created.id
            # and tells the provisioner to check Entra before deciding to retry,
            # since the user may already exist in the tenant.
            if created is None:
                raise GraphClientError(
                    f"create_user returned None for {payload.upn} — "
                    "Graph API call may have succeeded but returned no object. "
                    "Check Entra ID before retrying."
                )

            logger.info(f"User created — upn={payload.upn}, object_id={created.id}")

            return {"id": created.id, "upn": created.user_principal_name}

        except Exception as e:
            raise GraphClientError(f"create_user failed for {payload.upn}: {e}")

    def get_group(self, group_id: str) -> dict:
        """
        Retrieve a group by object ID.

        Returns group details including is_dynamic — the provisioner uses
        this to skip manual membership assignment for dynamic groups, since
        Entra manages their membership automatically via membership rules.

        Raises GraphClientError if the group is not found.
        """
        try:
            group = self._run(self._client.groups.by_group_id(group_id).get())

            if group is None:
                raise GraphClientError(f"Group not found: {group_id}")

            return {
                "id":              group.id,
                "display_name":    group.display_name,
                "membership_rule": group.membership_rule,
                "is_dynamic":      bool(group.membership_rule),  # Dynamic groups have a rule; static ones don't
            }

        except GraphClientError:
            raise
        except Exception as e:
            raise GraphClientError(f"get_group failed for {group_id}: {e}")

    def check_group_membership(self, user_id: str, group_id: str) -> bool:
        """
        Return True if the user is already a member of the group.

        Always called before add_group_member() to keep assignment idempotent.
        If the pipeline retries after a partial run, this prevents the same
        user being added twice (which would cause a Graph API conflict error).
        """
        try:
            members = self._run(
                self._client.groups.by_group_id(group_id).members.get()
            )

            if members and members.value:
                for member in members.value:
                    if member.id == user_id:
                        return True
            return False

        except Exception as e:
            raise GraphClientError(
                f"check_group_membership failed — user={user_id}, group={group_id}: {e}"
            )

    def add_group_member(self, user_id: str, group_id: str) -> None:
        """
        Add a user to an Entra ID group.

        Only called after check_group_membership() confirms the user is
        not already a member. The provisioner is responsible for that check —
        this method does not guard against duplicates itself.
        """
        try:
            ref          = ReferenceCreate()
            ref.odata_id = (
                f"https://graph.microsoft.com/v1.0/directoryObjects/{user_id}"
            )

            self._run(
                self._client.groups.by_group_id(group_id).members.ref.post(ref)
            )

            logger.info(f"Group member added — user={user_id}, group={group_id}")

        except Exception as e:
            raise GraphClientError(
                f"add_group_member failed — user={user_id}, group={group_id}: {e}"
            )

    def check_rbac_assignment(
        self,
        user_id: str,
        role_definition_id: str,
        scope: str
    ) -> bool:
        """
        Return True if the RBAC role assignment already exists for this user.

        Always called before create_rbac_assignment() for the same reason as
        check_group_membership — prevents duplicate assignments on retry and
        avoids a Graph conflict error if the pipeline crashed mid-run.
        """
        try:
            assignments = self._run(
                self._client.role_management.directory.role_assignments.get()
            )

            if assignments and assignments.value:
                for assignment in assignments.value:
                    if (
                        assignment.principal_id      == user_id
                        and assignment.role_definition_id == role_definition_id
                    ):
                        return True
            return False

        except Exception as e:
            raise GraphClientError(f"check_rbac_assignment failed — user={user_id}: {e}")

    def create_rbac_assignment(
        self,
        user_id: str,
        role_definition_id: str,
        scope: str
    ) -> None:
        """
        Assign an Entra ID directory role to a user.

        Only called after check_rbac_assignment() confirms the assignment
        does not already exist. The provisioner owns that check.

        Inputs:
            user_id            — Entra object ID of the provisioned user
            role_definition_id — ID of the role definition to assign
            scope              — directory scope, typically "/" for tenant-wide
        """
        try:
            from msgraph.generated.models.unified_role_assignment import (
                UnifiedRoleAssignment
            )

            assignment                    = UnifiedRoleAssignment()
            assignment.principal_id       = user_id
            assignment.role_definition_id = role_definition_id
            assignment.directory_scope_id = scope

            self._run(
                self._client.role_management.directory.role_assignments.post(assignment)
            )

            logger.info(
                f"RBAC assignment created — user={user_id}, "
                f"role={role_definition_id}, scope={scope}"
            )

        except Exception as e:
            raise GraphClientError(f"create_rbac_assignment failed — user={user_id}: {e}")


def _generate_temp_password(employee_id: str) -> str:
    """
    Produce a temporary password for new user creation.

    Deterministic — the same employee_id always produces the same password.
    This matters for retries: if the user was created but the pipeline crashed
    before recording the object ID, a retry can call create_user() again with
    the same temp password rather than generating a new unknown value.

    The password meets Entra ID complexity requirements (uppercase, lowercase,
    digit, special character). force_change_password_next_sign_in is set to
    True in create_user(), so this value is discarded after first login.
    """
    import hashlib
    suffix = hashlib.sha256(employee_id.encode()).hexdigest()[:8].upper()
    return f"JmlTmp!{suffix}1a"