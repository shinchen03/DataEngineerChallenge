package paypay.challenge

import java.sql.Timestamp

case class SessionAnalyzerSchema(
    timestamp: Timestamp,
    clientIP: String,
    elbStatusCode: Int,
    backendStatusCode: Int,
    url: String,
    userAgent: String,
    isNewSession: Boolean,
    sessionId: String
)
