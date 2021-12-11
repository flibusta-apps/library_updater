import httpx

from core.config import env_config, WebhookConfig


class WebhookSender:
    @classmethod
    async def _make_request(cls, webhook: WebhookConfig):
        async with httpx.AsyncClient() as client:
            request_maker= getattr(client, webhook.method)
            await request_maker(webhook.url, headers=webhook.headers)

    @classmethod
    async def send(cls):
        webhooks = env_config.WEBHOOKS

        if webhooks is None:
            return

        for webhook in webhooks:
            await cls._make_request(webhook)
