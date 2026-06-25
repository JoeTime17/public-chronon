import json
import os
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import click
import requests

from ai.chronon.cli.formatter import Format
from ai.chronon.cli.theme import print_error, print_info, print_warning
from ai.chronon.repo import utils


def _format_hub_partition(value, default_value):
    if value is None:
        return default_value
    if isinstance(value, datetime):
        if value.second != 0 or value.microsecond != 0:
            raise ValueError(f"Hub partition {value} must be aligned to minute precision")
        if value.time() == datetime.min.time():
            return value.strftime("%Y-%m-%d")
        return value.strftime("%Y-%m-%d-%H-%M")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value)


class ZiplineHub:
    def __init__(
        self,
        base_url,
        sa_name=None,
        use_auth=False,
        eval_url=None,
        cloud_provider=None,
        scope=None,
        format: Format = Format.TEXT,
        auth_url=None,
    ):
        if not base_url:
            raise ValueError("Base URL for ZiplineHub cannot be empty.")
        self.base_url = base_url.rstrip("/")
        self.eval_url = eval_url
        self.format = format
        self.cloud_provider = (
            cloud_provider.lower() if cloud_provider is not None else cloud_provider
        )
        self.version = utils.get_package_version()
        self._hub_auth_config = None
        self._zipline_token = None
        self._zipline_auth_url = None

        # Auth resolution order:
        # 0. ZIPLINE_TOKEN env var (session token → JWT exchange, or JWT directly)
        # 1. CLI auth exact match (auth_url matches a stored account)
        # 2. Cloud provider auth (GCP/Azure service account, ID_TOKEN)
        # 3. CLI auth default (fallback to default account for non-cloud setups)
        zipline_token = os.getenv("ZIPLINE_TOKEN")
        if zipline_token:
            self._zipline_token = zipline_token
            self._zipline_auth_url = os.getenv("ZIPLINE_AUTH_URL") or auth_url
            self.use_auth = True
            self.sa = None
            self.id_token = None
            print_info("Using ZIPLINE_TOKEN for authentication.", format=format)
            return

        if use_auth:
            from ai.chronon.repo.auth import get_auth_config

            config = get_auth_config(url=auth_url)
            if config and config.get("access_token"):
                self._hub_auth_config = config
                self.use_auth = True
                self.sa = None
                self.id_token = None
                print_info("Using authentication for ZiplineHub.", format=format)
                return

        if self.base_url.startswith("https") and use_auth:
            if self.cloud_provider == "gcp":
                self.use_auth = True
                self._setup_gcp_auth(sa_name)
            elif self.cloud_provider == "azure":
                if not scope:
                    raise ValueError("Azure auth requires a non-empty scope.")
                self.use_auth = True
                self.sa = None
                self._setup_azure_auth(scope)
            else:
                # For non-GCP clouds, check for generic token
                self.id_token = os.getenv("ID_TOKEN")
                if not self.id_token:
                    # Disable auth if ID_TOKEN is not available for non-GCP clouds
                    self.use_auth = False
                    self.id_token = None
                    self.sa = None
                    print_warning(
                        "No ID_TOKEN found in environment for non-GCP cloud provider. Disabling authentication.",
                        format=format,
                    )
                else:
                    self.use_auth = True
                    print_info("Using authentication for ZiplineHub.", format=format)
                    self.sa = None
        elif use_auth:
            # No cloud auth available — try default CLI auth as fallback
            # (e.g., local dev where FRONTEND_URL in teams.py differs from the auth URL)
            from ai.chronon.repo.auth import get_auth_config

            config = get_auth_config()
            if config and config.get("access_token"):
                self._hub_auth_config = config
                self.use_auth = True
                self.sa = None
                self.id_token = None
                print_info("Using authentication for ZiplineHub.", format=format)
                return
            self.use_auth = False
            print_info("Not using authentication for ZiplineHub.", format=format)
        else:
            self.use_auth = False
            print_info("Not using authentication for ZiplineHub.", format=format)


    def _setup_gcp_auth(self, sa_name):
        """Setup Google Cloud authentication."""
        import google.auth
        from google.auth.exceptions import GoogleAuthError
        from google.auth.transport.requests import Request

        print_info("Using Google Cloud authentication for ZiplineHub.", format=self.format)

        # First try to get ID token from environment (GitHub Actions)
        self.id_token = os.getenv("GCP_ID_TOKEN")
        if self.id_token:
            print_info("Using ID token from environment.", format=self.format)
            self.sa = None
        elif sa_name is not None:
            # Fallback to Google Cloud authentication
            print_info("Generating ID token from service account credentials.", format=self.format)
            try:
                credentials, project_id = google.auth.default()
                self.project_id = project_id
                credentials.refresh(Request())
            except GoogleAuthError as e:
                raise click.ClickException(str(e)) from e

            self.sa = f"{sa_name}@{project_id}.iam.gserviceaccount.com"
        else:
            print_info("Generating ID token from default credentials.", format=self.format)
            try:
                credentials, project_id = google.auth.default()
                credentials.refresh(Request())
            except GoogleAuthError as e:
                raise click.ClickException(str(e)) from e
            self.sa = None
            self.id_token = credentials.id_token

    def _setup_azure_auth(self, scope):
        """Setup Azure authentication."""
        from azure.core.exceptions import ClientAuthenticationError
        from azure.identity import AzureCliCredential, CredentialUnavailableError

        print_info("Using Azure authentication for ZiplineHub.", format=self.format)
        print_info(
            f"Acquiring token from CLI credentials for scope: {scope}...", format=self.format
        )
        try:
            credential = AzureCliCredential()

            # Request the token
            token_object = credential.get_token(scope)
            self.id_token = token_object.token

            print_info("Token acquired.", format=self.format)
        except (ClientAuthenticationError, CredentialUnavailableError) as e:
            print_warning(
                f"Could not acquire token. Make sure you are logged in via 'az login'. Details: {e}",
                format=self.format,
            )
            self.use_auth = False
            self.id_token = None
            return

    def additional_headers(self, url):
        headers = {
            "Content-Type": "application/json",
            "X-Zipline-Version": self.version,
        }
        if self._zipline_token:
            from ai.chronon.repo.token_exchange import resolve_token

            try:
                jwt_token = resolve_token(self._zipline_token, self._zipline_auth_url)
            except (RuntimeError, ValueError) as e:
                raise click.ClickException(str(e)) from None
            headers["Authorization"] = f"Bearer {jwt_token}"
        elif self._hub_auth_config:
            jwt_token = self._get_hub_jwt()
            if jwt_token:
                headers["Authorization"] = f"Bearer {jwt_token}"
        elif self.use_auth and hasattr(self, "sa") and self.sa is not None:
            headers["Authorization"] = f"Bearer {self._sign_jwt(self.sa, url)}"
        elif self.use_auth:
            headers["Authorization"] = f"Bearer {self.id_token}"
        return headers

    def _get_hub_jwt(self):
        """Fetch a short-lived JWT from Zipline Hub using the stored session token."""
        config = self._hub_auth_config
        hub_url = config["url"]
        session_token = config["access_token"]
        try:
            resp = requests.get(
                f"{hub_url}/api/auth/token",
                headers={"Authorization": f"Bearer {session_token}"},
                timeout=10,
            )
            if resp.ok:
                return resp.json().get("token")
            else:
                print_warning(
                    "Session expired. Run 'zipline auth login' to re-authenticate.",
                    format=self.format,
                )
                return None
        except requests.RequestException as e:
            print_warning(f"Failed to fetch JWT from hub: {e}", format=self.format)
            return None

    def _get_error_details(self, e: requests.RequestException) -> str:
        """Extract error details from request exception including response JSON if available."""
        error_parts = [str(e)]
        if e.response is not None:
            try:
                response_json = e.response.json()
                error_parts.append(f"Response: {response_json}")
            except (ValueError, AttributeError):
                # Response is not JSON or cannot be parsed
                pass
        return " | ".join(error_parts)

    def handle_unauth(self, e: requests.RequestException, api_name: str):
        if e.response is None or e.response.status_code != 401:
            return
        sa = getattr(self, "sa", None)
        if self._zipline_token:
            msg = (
                f"Error calling {api_name} API. Unauthorized with ZIPLINE_TOKEN. "
                "The token may be expired or revoked. If using a service principal, "
                "rotate the token in the admin UI. If using a JWT from "
                "'zipline auth get-access-token', it may have expired (~15 min)."
            )
        elif self._hub_auth_config:
            msg = (
                f"Error calling {api_name} API. Session expired or invalid. "
                "Run 'zipline auth login' to re-authenticate."
            )
        elif not self.use_auth:
            msg = (
                f"Error calling {api_name} API. Unauthorized. "
                "Run 'zipline auth login' to authenticate."
            )
        elif sa is not None:
            msg = (
                f"Error calling {api_name} API. Unauthorized with service account: {sa}. "
                "Ensure the service account has 'iap.webServiceVersions.accessViaIap' permission."
            )
        else:
            msg = (
                f"Error calling {api_name} API. Unauthorized. "
                "Set up default credentials, provide SA_NAME in teams.py, or run 'zipline auth login'."
            )
        raise click.ClickException(msg) from e

    def _generate_jwt_payload(self, service_account_email: str, resource_url: str) -> str:
        """Generates JWT payload for service account.

        Creates a properly formatted JWT payload with standard claims (iss, sub, aud,
        iat, exp) needed for IAP authentication.

        Args:
            service_account_email (str): Specifies service account JWT is created for.
            resource_url (str): Specifies scope of the JWT, the URL that the JWT will
                be allowed to access.

        Returns:
            str: JSON string containing the JWT payload with properly formatted claims.
        """
        # Create current time and expiration time (1 hour later) in UTC
        iat = datetime.now(tz=timezone.utc)
        exp = iat + timedelta(seconds=3600)

        # Convert datetime objects to numeric timestamps (seconds since epoch)
        # as required by JWT standard (RFC 7519)
        payload = {
            "iss": service_account_email,
            "sub": service_account_email,
            "aud": resource_url,
            "iat": int(iat.timestamp()),
            "exp": int(exp.timestamp()),
        }

        return json.dumps(payload)

    def _sign_jwt(self, target_sa: str, resource_url: str) -> str:
        """Signs JWT payload using ADC and IAM credentials API.

        Uses Google Cloud's IAM Credentials API to sign a JWT. This requires the
        caller to have iap.webServiceVersions.accessViaIap permission on the target
        service account.

        Args:
            target_sa (str): Service Account JWT is being created for.
                iap.webServiceVersions.accessViaIap permission is required.
            resource_url (str): Audience of the JWT, and scope of the JWT token.
                This is the url of the IAP protected application.

        Returns:
            str: A signed JWT that can be used to access IAP protected apps.
                Use in Authorization header as: 'Bearer <signed_jwt>'
        """
        import google.auth
        from google.cloud import iam_credentials_v1

        # Get default credentials from environment or application credentials
        source_credentials, project_id = google.auth.default()

        # Initialize IAM credentials client with source credentials
        iam_client = iam_credentials_v1.IAMCredentialsClient(credentials=source_credentials)

        # Generate the service account resource name
        # Use '-' as placeholder as per API requirements
        name = iam_client.service_account_path("-", target_sa)

        # Create and sign the JWT payload
        payload = self._generate_jwt_payload(target_sa, resource_url)

        request = iam_credentials_v1.SignJwtRequest(
            name=name,
            payload=payload,
        )
        # Sign the JWT using the IAM credentials API
        response = iam_client.sign_jwt(request=request)

        return response.signed_jwt

    def call_diff_api(self, names_to_hashes: dict[str, str]) -> Optional[list[str]]:
        url = f"{self.base_url}/upload/v2/diff"

        diff_request = {"namesToHashes": names_to_hashes}
        try:
            response = requests.post(
                url, json=diff_request, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling diff API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "diff")
            print_error(f"Error calling diff API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_upload_api(self, diff_confs, branch: str):
        url = f"{self.base_url}/upload/v2/confs"

        upload_request = {
            "diffConfs": diff_confs,
            "branch": branch,
        }

        try:
            response = requests.post(
                url, json=upload_request, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling upload API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "upload")
            print_error(f"Error calling upload API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_schedule_api(self, modes, branch, conf_name, conf_hash):
        url = f"{self.base_url}/schedule/v2/schedules"

        schedule_request = {
            "modeSchedules": modes,
            "branch": branch,
            "confName": conf_name,
            "confHash": conf_hash,
        }

        try:
            response = requests.post(
                url, json=schedule_request, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error deploying schedule: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "schedule deploy")
            print_error(f"Error deploying schedule: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_schedule_all_api(self, schedules: list[dict]) -> dict:
        """
        Deploy schedules for multiple confs in a single batch request.

        Args:
            schedules: List of dicts with keys:
                - conf_name: str
                - conf_hash: str
                - branch: str
                - modes: dict[str, str] (mode name -> schedule expression)

        Returns:
            dict with:
                - results: list[dict] with per-conf results
                - totalCount: int
                - successCount: int
                - failureCount: int
        """
        url = f"{self.base_url}/schedule/v2/schedules/all"

        # Transform to thrift structure format
        schedule_items = [
            {
                "confName": s["conf_name"],
                "confHash": s["conf_hash"],
                "branch": s["branch"],
                "modeSchedules": s["modes"],
            }
            for s in schedules
        ]

        request_body = {"schedules": schedule_items}

        try:
            response = requests.post(
                url, json=request_body, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error deploying schedules: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "schedule-all")
            print_error(f"Error deploying schedules: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_cancel_api(self, workflow_id):
        url = f"{self.base_url}/workflow/v2/{workflow_id}/cancel"

        try:
            response = requests.post(url, headers=self.additional_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling workflow cancel API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "workflow cancel")
            print_error(f"Error calling workflow cancel API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_streaming_redeploy_api(self, metadata_names: list[str]) -> dict:
        url = f"{self.base_url}/streaming/v1/redeploy"
        request_body = {"metadataNames": metadata_names}
        try:
            response = requests.post(url, json=request_body, headers=self.additional_headers(self.base_url))
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling streaming redeploy API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format,
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "streaming redeploy")
            print_error(f"Error calling streaming redeploy API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_sync_api(self, branch: str, names_to_hashes: dict[str, str]) -> Optional[list[str]]:
        url = f"{self.base_url}/upload/v2/sync"

        sync_request = {
            "namesToHashes": names_to_hashes,
            "branch": branch,
        }

        try:
            response = requests.post(
                url, json=sync_request, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling sync API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "sync")
            print_error(f"Error calling sync API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_eval_api(
        self,
        conf_name,
        conf_hash_map,
        parameters=None,
    ):
        if not self.eval_url:
            raise ValueError(
                "Eval URL not specified. Specify EVAL_URL in teams.py, environment variables, or use --eval-url."
            )
        _request = {
            "confName": conf_name,
            "confHashMap": conf_hash_map,
        }
        if parameters:
            _request["parameters"] = parameters
        try:
            response = requests.post(
                self.eval_url + "/eval", json=_request, headers=self.additional_headers(self.eval_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling eval API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "eval")
            print_error(f"Error calling eval API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_schema_api(
        self,
        table_name,
        engine_type,
        execution_info,
    ):
        if not self.eval_url:
            raise ValueError(
                "Eval URL not specified. Specify EVAL_URL in teams.py, environment variables, or use --eval-url."
            )
        _request = {
            "tableName": table_name,
            "engineType": engine_type,
            "executionInfo": execution_info,
        }
        try:
            response = requests.post(
                self.eval_url + "/schema", json=_request, headers=self.additional_headers(self.eval_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling schema API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "schema")
            print_error(f"Error calling schema API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_list_tables_api(
        self,
        schema_name,
        engine_type,
        execution_info,
    ):
        if not self.eval_url:
            raise ValueError(
                "Eval URL not specified. Specify EVAL_URL in teams.py, environment variables, or use --eval-url."
            )
        _request = {
            "schemaName": schema_name,
            "engineType": engine_type,
            "executionInfo": execution_info,
        }
        try:
            response = requests.post(
                self.eval_url + "/list-tables", json=_request, headers=self.additional_headers(self.eval_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling list-tables API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "list-tables")
            print_error(f"Error calling list-tables API: {self._get_error_details(e)}", format=self.format)
            raise e

    def call_workflow_start_api(
        self,
        conf_name,
        mode,
        branch,
        user,
        conf_hash,
        start=None,
        end=None,
        skip_long_running=False,
        concurrency=None,
    ):
        url = f"{self.base_url}/workflow/v2/start"
        end_dt = _format_hub_partition(end, date.today().strftime("%Y-%m-%d"))
        start_dt = _format_hub_partition(start, (date.today() - timedelta(days=14)).strftime("%Y-%m-%d"))
        workflow_request = {
            "confName": conf_name,
            "confHash": conf_hash,
            "mode": mode,
            "branch": branch,
            "user": user,
            "start": start_dt,
            "end": end_dt,
            "skipLongRunningNodes": skip_long_running,
        }
        if concurrency is not None:
            workflow_request["workflowConcurrency"] = concurrency
        try:
            response = requests.post(
                url, json=workflow_request, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling workflow start API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "workflow start")
            print_error(f"Error calling workflow start API: {self._get_error_details(e)}", format=self.format)
            raise e

    def preview_clear_downstream(self, conf_name, branch, user, start, end):
        url = f"{self.base_url}/workflow/v2/clear-downstream/preview"
        start_dt = _format_hub_partition(start, None)
        end_dt = _format_hub_partition(end, None)
        clear_request = {
            "confName": conf_name,
            "branch": branch,
            "user": user,
            "start": start_dt,
            "end": end_dt,
        }
        try:
            response = requests.post(
                url, json=clear_request, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling clear-downstream preview API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format,
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "clear-downstream/preview")
            print_error(f"Error calling clear-downstream preview API: {self._get_error_details(e)}", format=self.format)
            raise e

    def apply_clear_downstream(self, node_results, user, affected_confs=None):
        url = f"{self.base_url}/workflow/v2/clear-downstream/apply"
        apply_request = {
            "nodeResults": node_results,
            "user": user,
        }
        if affected_confs:
            apply_request["affectedConfs"] = affected_confs
        try:
            response = requests.post(
                url, json=apply_request, headers=self.additional_headers(self.base_url)
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.JSONDecodeError as e:
            print_error(
                f"Error calling clear-downstream apply API: Invalid JSON response\n"
                f"Response status: {response.status_code}\n"
                f"Response text: {response.text[:500]}",
                format=self.format,
            )
            raise e
        except requests.RequestException as e:
            self.handle_unauth(e, "clear-downstream/apply")
            print_error(f"Error calling clear-downstream apply API: {self._get_error_details(e)}", format=self.format)
            raise e
