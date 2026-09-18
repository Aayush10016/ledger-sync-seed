package in.simplifymoney.ledgersync.model;

import java.time.OffsetDateTime;

/**
 * One SMS or email exactly as the mobile client uploaded it.
 *
 * messageId is assigned by the client at upload time. It identifies THIS UPLOAD,
 * not the underlying message: the same SMS re-read from the inbox is uploaded
 * again with a new messageId.
 */
public record RawMessage(
        String messageId,
        String channel,          // "sms" | "email"
        String sender,
        OffsetDateTime receivedAt,
        String deviceId,
        String body) {
        
    public RawMessage {
        if (messageId == null || messageId.isBlank()) {
            throw new IllegalArgumentException("messageId must be present and non-empty");
        }
        if (channel == null || (!channel.equals("sms") && !channel.equals("email"))) {
            throw new IllegalArgumentException("channel must be 'sms' or 'email'");
        }
        if (receivedAt == null) {
            throw new IllegalArgumentException("receivedAt must be a valid timestamp");
        }
        if (body == null || body.isBlank()) {
            throw new IllegalArgumentException("body must be present");
        }
    }
}
