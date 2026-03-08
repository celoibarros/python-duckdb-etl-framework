# -*- coding: utf-8 -*-
import logging
from typing import Any, Optional

from azure.core.credentials import AccessToken
from azure.identity import (AzureCliCredential, ClientSecretCredential,
                            EnvironmentCredential,
                            InteractiveBrowserCredential,
                            ManagedIdentityCredential)


class ChainedTokenCredential:
    def __init__(self, *credentials):
        # type: (*TokenCredential) -> None
        if not credentials:
            raise ValueError("at least one credential is required")

        self.credentials = credentials

    def get_token(
        self,
        *scopes: str,
        claims: Optional[str] = None,
        tenant_id: Optional[str] = None,
        **kwargs: Any,
    ) -> AccessToken:
        """Request a token from each chained credential, in order, returning the first token received.

        This method is called automatically by Azure SDK clients.

        :param str scopes: desired scopes for the access token. This method requires at least one scope.
            For more information about scopes, see
            https://learn.microsoft.com/entra/identity-platform/scopes-oidc.
        :keyword str claims: additional claims required in the token, such as those returned in a resource provider's
            claims challenge following an authorization failure.
        :keyword str tenant_id: optional tenant to include in the token request.

        :return: An access token with the desired scopes.
        :rtype: ~azure.core.credentials.AccessToken
        :raises ~azure.core.exceptions.ClientAuthenticationError: no credential in the chain provided a token
        """
        from azure.core.exceptions import ClientAuthenticationError

        for credential in self.credentials:
            try:
                token = credential.get_token(
                    *scopes, claims=claims, tenant_id=tenant_id, **kwargs
                )
                self._successful_credential = credential
                return token
            except Exception as ex:  # pylint: disable=broad-except
                logging.info(
                    f'{self.__class__.__name__}.get_token failed: {credential.__class__.__name__} raised unexpected error "{ex}"'
                )
        raise ClientAuthenticationError(message="Error")

    async def close(self):
        # Dummy async close method for fsspec/adlfs compatibility
        return


class TokenCredential:
    def __init__(self, token):
        self.token = token

    def get_token(self, *scopes, **kwargs):
        if not self.token:
            raise Exception("No token defined.")

        return AccessToken(self.token, float("inf"))

    async def close(self):
        # TokenCredential has no inner chain; keep async method for interface compatibility.
        return


class AzureCredential:
    def __init__(
        self,
        tenant_id=None,
        managed_identity_id=None,
        token=None,
        credentials=None,
        client_id=None,
        client_secret=None,
    ):
        if not credentials:
            environment_credential = EnvironmentCredential()
            managed_credential = (
                ManagedIdentityCredential(client_id=managed_identity_id)
                if managed_identity_id
                else None
            )
            cli_credential = AzureCliCredential(tenant_id=tenant_id)
            secret_credential = (
                ClientSecretCredential(tenant_id, client_id, client_secret)
                if client_id and client_secret
                else None
            )
            browser_credential = InteractiveBrowserCredential(tenant_id=tenant_id)
            token_credential = TokenCredential(token=token) if token else None
            credentials = (
                token_credential,
                managed_credential,
                environment_credential,
                cli_credential,
                secret_credential,
                browser_credential,
            )

        self.credential = ChainedTokenCredential(*credentials)

    def get_credential(self):
        return self.credential

    async def close(self):
        if hasattr(self.credential, "close"):
            await self.credential.close()


# Backward-compatible alias retained for existing imports/usages.
JTAChainedTokenCredential = ChainedTokenCredential
