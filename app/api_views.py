from enum import Enum

from apps.api.permissions import IsAuthenticated
from apps.notifications.serializers import (
    SmsNotificationSerializer,
    NotificationSerializer,
    EmailNotificationSerializer,
)
from notifications.service import SmsNotification, NotificationDataClass, EmailNotification, NotificationService
from cybernet import settings
from rest_framework.generics import GenericAPIView
from rest_framework.response import Response
from models import Lead, Notifications

class SmsView(GenericAPIView):
    """This django view simulates notifications sending system to the user (Lead)."""
    class ProviderChoices(Enum):
        SMS = "sms"
        EMAIL = "email"

    permission_classes = (IsAuthenticated,)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._provider = "sms"

    def get_queryset(self):
        return Lead.objects.filter(owner_id=self.kwargs["owner_id"])

    def get_provider(self) -> NotificationService:
        if self._provider == SmsView.ProviderChoices.EMAIL.value:
            return EmailNotification()
        return SmsNotification()

    def setup(self, request, *args, **kwargs):
        super().setup(request, *args, **kwargs)

        path = self.request.path

        if path.split("/")[-1] == "email":
            self._provider = "email"

    def get_serializer_class(self):
        if self._provider == SmsView.ProviderChoices.EMAIL.value:
            return EmailNotificationSerializer
        return SmsNotificationSerializer

    def get_serializer(self, *args, **kwargs):
        serializer_class = self.get_serializer_class()
        return serializer_class(*args, **kwargs)

    def post(self, request, *args, **kwargs):
        lead = self.get_object()

        serializer = self.get_serializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        data: NotificationDataClass = self.get_provider().send(lead, serializer.validated_data)

        notification = data.notification
        notification.save()
        notification = NotificationSerializer(data.notification).data

        metadata = data.meta_data

        return Response({"message": metadata["message"], "notification": notification}, status=metadata["code"])

    def get(self, request, *args, **kwargs):
        lead = self.get_object()
        notifications = Notification.objects.filter(lead=lead, notification_type=self._provider)
        serializer = NotificationSerializer(notifications, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)
