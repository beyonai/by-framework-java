package com.iwhaleai.byai.framework.worker;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * The inbound half of a resumed execution's metadata.
 *
 * <p>Two directions restore metadata on a resume, and they are NOT the same
 * rule. Keep them apart:
 *
 * <ul>
 *   <li><b>Outbound</b> ({@code GatewayWorker.resolveReplyCommand}): the header a
 *       resumed execution <i>sends</i> to its caller. The stored dispatch
 *       metadata <b>replaces</b> the header's wholesale — the waking hop's data
 *       is plumbing the original caller never asked for, and
 *       {@code enqueueAgentReturn}'s {@code header.metadata + taskResult.metadata}
 *       merge is the one sanctioned channel for a handler to forward part of it.
 *   <li><b>Inbound</b> (this class): the header a resumed execution's own handler
 *       <i>reads</i>. Here the waking message's metadata is legitimate payload —
 *       an askUser answer's metadata was sent BY a client TO this agent — so it
 *       is merged on top of the original dispatch metadata rather than discarded.
 * </ul>
 *
 * <p>Without this, everything a handler was originally dispatched with
 * disappears the first time it suspends: it comes back seeing only whatever woke
 * it up.
 *
 * <p>Mirrors Python {@code worker/_resume_metadata.py} and TS
 * {@code src/resume_metadata.ts}. One copy even though Java currently has a
 * single call site: Python and TS each shipped this rule duplicated across their
 * worker and processor paths and each needed a follow-up commit to fix the copy
 * they missed, and the wait sweeper will become a second reader here.
 */
public final class ResumeMetadata {

    /**
     * Framework-injected, per-hop keys. A stored copy is stale by definition — it
     * describes the hop that <i>dispatched</i> the execution, not the hop
     * resuming it now — so they are dropped from the restored base and always
     * come from the current message.
     *
     * <p>Java's own exposure differs from the other SDKs, and the difference is
     * worth knowing before anyone "simplifies" this away. Java lifts
     * {@code trace_parent_span_id} and {@code langfuse_parent_observation_id} out
     * of metadata into typed MessageHeader fields at dispatch time, and resolves
     * no trace parent from metadata anywhere, so a Java-only deployment never
     * puts these keys in metadata at all and the filter is a no-op.
     *
     * <p>It is <b>not</b> a no-op in a mixed deployment: a Python caller
     * setdefaults exactly these keys INTO {@code header.metadata}, so a Python
     * parent dispatching a Java child leaves that hop's span ids in Java's
     * execution record. Restoring them would hand the handler a span id from
     * before the suspend.
     */
    public static final Set<String> FRAMEWORK_HOP_METADATA_KEYS = Set.of(
            "trace_parent_span_id",
            "framework_parent_span_id",
            "langfuse_parent_observation_id");

    private ResumeMetadata() {
    }

    /**
     * Merge an execution's original dispatch metadata under this hop's.
     *
     * <p>{@code stored} is the {@code metadata} field of the execution record
     * (what the execution was originally dispatched with); {@code incoming} is
     * the waking message's own metadata. The waking message wins on key
     * collisions: it is the newer, more specific hop, and this keeps the property
     * that every key a handler can read today stays readable — the restore only
     * ever adds keys.
     *
     * <p>A {@code stored} that is missing (an execution recorded before this
     * field existed, or one dispatched by an SDK that does not write it) degrades
     * to {@code incoming} unchanged, i.e. exactly the pre-restore behaviour.
     *
     * <p>Neither argument is mutated, and the result is a fresh mutable map.
     */
    public static Map<String, Object> mergeResumeMetadata(
            Map<String, Object> stored, Map<String, Object> incoming) {
        Map<String, Object> merged = new LinkedHashMap<>();
        if (stored != null) {
            for (Map.Entry<String, Object> entry : stored.entrySet()) {
                if (!FRAMEWORK_HOP_METADATA_KEYS.contains(entry.getKey())) {
                    merged.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (incoming != null) {
            merged.putAll(incoming);
        }
        return merged;
    }

    /**
     * Read an execution record's {@code metadata} field as a map.
     *
     * <p>The record is decoded JSON, so the field is an untyped Object that may
     * be absent or, in a corrupted record, not a map at all. Both degrade to an
     * empty map rather than throwing: losing the restore costs metadata, whereas
     * throwing here would cost the reply.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> storedMetadata(Map<String, Object> executionRecord) {
        if (executionRecord == null) {
            return Collections.emptyMap();
        }
        Object raw = executionRecord.get("metadata");
        if (raw instanceof Map) {
            return new HashMap<>((Map<String, Object>) raw);
        }
        return Collections.emptyMap();
    }
}
