package com.iwhaleai.byai.framework.core.liveness;

import com.iwhaleai.byai.framework.common.Constants;
import com.iwhaleai.byai.framework.core.protocol.GatewayCommand;
import com.iwhaleai.byai.framework.core.protocol.MessageHeader;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Wait-index member encoding and shard selection.
 *
 * <p><b>Every byte here is a cross-SDK contract.</b> Python, TypeScript and Java
 * all write into and read out of the same ZSETs, so a divergence in the hash, the
 * escaping, or the field order does not fail loudly — it makes the gate compute a
 * member the other side never wrote, and the two sides silently stop seeing each
 * other's entries.
 *
 * <p>Contract: {@code research/cross-sdk-wire-contract.md} sections 2 and 3.
 */
public final class WaitIndex {

    private static final char SEPARATOR = '|';
    private static final char ESCAPE = '\\';

    private WaitIndex() {
    }

    /** The four identity fields a wait-index member encodes. */
    public record Member(
            String sessionId,
            String parentMessageId,
            String childMessageId,
            String taskGroupId) {
    }

    /**
     * Stable short id for a member, for keys that are named after one.
     *
     * <p>The member is hashed rather than embedded because it is built from
     * caller-controlled ids of unbounded length. SHA-1 hex for the same reason as
     * FNV-1a: trivially reproducible across SDKs, and every key derived from it is
     * cross-SDK contract surface.
     */
    public static String memberDigest(String member) {
        try {
            java.security.MessageDigest md = java.security.MessageDigest.getInstance("SHA-1");
            byte[] hash = md.digest((member == null ? "" : member).getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-1 unavailable", e);
        }
    }

    /**
     * FNV-1a 32-bit over UTF-8.
     *
     * <p><b>Deliberately not {@code String.hashCode()}</b>, which is a different
     * algorithm entirely. Using it would send Java to a different shard than the
     * Python or TS worker that registered the wait, so neither side would ever
     * find the other's entries — with no error anywhere.
     */
    public static long fnv1a32(String text) {
        long digest = 0x811C9DC5L;
        for (byte b : (text == null ? "" : text).getBytes(StandardCharsets.UTF_8)) {
            digest = ((digest ^ (b & 0xFF)) * 0x01000193L) & 0xFFFFFFFFL;
        }
        return digest;
    }

    /** Shard owning a session's wait entries. */
    public static int shardForSession(String sessionId) {
        return (int) (fnv1a32(sessionId) % Constants.WAIT_INDEX_SHARDS);
    }

    /**
     * Encode the four identity fields into a ZSET member.
     *
     * <p>Format: {@code {sessionId}|{parentMessageId}|{childMessageId}|{taskGroupId}}.
     *
     * <p>Framework-minted ids ({@code msg-}/{@code tg-} plus hex) never contain the
     * separator, but {@code sessionId} and a caller-supplied {@code messageId} are
     * arbitrary caller-controlled strings, so {@code \} and {@code |} are escaped
     * rather than assumed absent. Order matters: {@code \} first, then {@code |}.
     */
    public static String encodeMember(String sessionId, String parentMessageId,
            String childMessageId, String taskGroupId) {
        StringBuilder sb = new StringBuilder();
        appendEscaped(sb, sessionId);
        sb.append(SEPARATOR);
        appendEscaped(sb, parentMessageId);
        sb.append(SEPARATOR);
        appendEscaped(sb, childMessageId);
        sb.append(SEPARATOR);
        appendEscaped(sb, taskGroupId);
        return sb.toString();
    }

    private static void appendEscaped(StringBuilder sb, String value) {
        String v = value == null ? "" : value;
        for (int i = 0; i < v.length(); i++) {
            char c = v.charAt(i);
            if (c == ESCAPE || c == SEPARATOR) {
                sb.append(ESCAPE);
            }
            sb.append(c);
        }
    }

    /**
     * Inverse of {@link #encodeMember}.
     *
     * <p>Decodes by scanning characters, <b>not</b> by splitting on the separator:
     * a split would cut an escaped {@code \|} in half and silently produce five
     * fields out of four.
     *
     * @throws IllegalArgumentException on a malformed member
     */
    public static Member decodeMember(String encoded) {
        List<String> fields = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean escaped = false;
        String source = encoded == null ? "" : encoded;

        for (int i = 0; i < source.length(); i++) {
            char c = source.charAt(i);
            if (escaped) {
                current.append(c);
                escaped = false;
            } else if (c == ESCAPE) {
                escaped = true;
            } else if (c == SEPARATOR) {
                fields.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (escaped) {
            throw new IllegalArgumentException(
                    "Invalid wait-index member (dangling escape): " + encoded);
        }
        fields.add(current.toString());

        if (fields.size() != 4) {
            throw new IllegalArgumentException(
                    "Invalid wait-index member (expected 4 fields, got " + fields.size()
                            + "): " + encoded);
        }
        return new Member(fields.get(0), fields.get(1), fields.get(2), fields.get(3));
    }

    /**
     * Rebuild the member a reply is meant to clear.
     *
     * <p>The direction reversal is the whole point: a reply's
     * {@code header.messageId} is the <i>caller's</i> id and its
     * {@code header.parentMessageId} is the <i>sub-task's</i> dispatch-time id.
     * Keying by the reply's own message id instead would make every sibling in a
     * Task Group collide.
     *
     * <p>All four fields come from the ResumeCommand alone — that is what lets the
     * gate run without an extra lookup, and it is why no field a replier cannot
     * know (a per-call timeout, say) may ever be added here.
     */
    public static String memberFromResume(GatewayCommand command) {
        MessageHeader header = command.header();
        return encodeMember(
                header.sessionId(),
                header.messageId(),
                header.parentMessageId(),
                header.taskGroupId());
    }
}
