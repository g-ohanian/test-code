from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Dict

from django.conf import settings
from rest_framework.exceptions import ValidationError
from rest_framework.status import HTTP_200_OK, HTTP_500_INTERNAL_SERVER_ERROR
from twilio.rest import Client as Twilio

from apps.leads.models import Lead
from apps.notifications.models import Notification
from apps.notifications.serializers import NotificationSerializer


@dataclass
class NotificationDataClass:
    meta_data: Dict
    notification: Notification


class NotificationService(ABC):
    NOTIFICATION_TYPE = ""

    @abstractmethod
    def send(self, lead: Lead, data: dict) -> NotificationDataClass:
        pass

    def _build_notification(self, notification_data: dict) -> Notification:
        if not self.NOTIFICATION_TYPE:
            raise ValidationError("Notification type is required.")

        notification_data["notification_type"] = self.NOTIFICATION_TYPE
        notification_data["send_date"] = datetime.now()

        serializer = NotificationSerializer(data=notification_data)
        serializer.is_valid(raise_exception=True)
        notification = Notification(**serializer.data)

        return notification


class SmsNotification(NotificationService):
    NOTIFICATION_TYPE = "sms"

    def __init__(self):
        self._client = Twilio(settings.TWILIO_ACCOUNT_SID, settings.TWILIO_AUTH_TOKEN)

    def send(self, lead: Lead, data: dict) -> NotificationDataClass:
        if not lead.phone:
            raise ValidationError(detail="Invalid phone number format.")

        body = data["message"]
        status = Notification.NotificationStatusChoices.Sent
        message = "SMS sent successfully."
        code = HTTP_200_OK
        message_sid = None

        notification = {
            "phone_number": lead.phone,
            "from_number": settings.TWILIO_PHONE_NUMBER,
            "sender": "Twilio",
            "lead": lead,
            "notification_text": body,
            "status": status,
            "message_sid": message_sid,
        }

        try:
            message = self._client.messages.create(body=message, from_=settings.TWILIO_PHONE_NUMBER, to=to)
            notification["message_sid"] = message.sid
        except Exception as e:
            status = Notification.NotificationStatusChoices.Failed
            message = str(e)
            code = HTTP_500_INTERNAL_SERVER_ERROR

            notification.update({"status": status, "error_message": message, "error_code": code})

        notification = self._build_notification(notification)

        return NotificationDataClass(
            notification=notification, meta_data={"message": message, "code": code, "status": status}
        )


class EmailNotification(NotificationService):
    pass
