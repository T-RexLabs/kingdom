ALTER TABLE provider_completion_settlements ADD COLUMN final_text_ref_json TEXT NOT NULL DEFAULT '';
ALTER TABLE provider_completion_settlements ADD COLUMN final_text_preview TEXT NOT NULL DEFAULT '';

-- Restore previously promoted content to chunk storage before retiring the
-- scalar-field alias. This migration is intentionally separate until squash.
WITH RECURSIVE pieces(invocation_id, sha256, ordinal, size_bytes, settlement_id, field) AS (
  SELECT c.provider_invocation_id, c.sha256, 0, c.size_bytes, r.settlement_id, r.field
  FROM provider_contents c JOIN provider_content_settlements r
    ON r.provider_invocation_id=c.provider_invocation_id AND r.sha256=c.sha256
  WHERE c.size_bytes > 0
  UNION ALL
  SELECT invocation_id, sha256, ordinal+1, size_bytes, settlement_id, field FROM pieces
  WHERE (ordinal+1)*65536 < size_bytes
)
INSERT OR IGNORE INTO provider_content_chunks(provider_invocation_id,sha256,ordinal,content)
SELECT p.invocation_id,p.sha256,p.ordinal,
  substr(CAST(CASE p.field WHEN 'final_text' THEN s.final_text ELSE s.provider_diagnostics_json END AS BLOB),p.ordinal*65536+1,65536)
FROM pieces p JOIN provider_completion_settlements s ON s.settlement_id=p.settlement_id;

UPDATE provider_completion_settlements AS s SET
  final_text_ref_json=(SELECT json_object('invocation_id',c.provider_invocation_id,'sha256',c.sha256,'size_bytes',c.size_bytes)
    FROM provider_content_settlements r JOIN provider_contents c
      ON c.provider_invocation_id=r.provider_invocation_id AND c.sha256=r.sha256
    WHERE r.settlement_id=s.settlement_id AND r.field='final_text'),
  final_text_preview=substr(COALESCE(final_text,''),1,8192),
  final_text=''
WHERE EXISTS (SELECT 1 FROM provider_content_settlements r JOIN provider_contents c
  ON c.provider_invocation_id=r.provider_invocation_id AND c.sha256=r.sha256
  WHERE r.settlement_id=s.settlement_id AND r.field='final_text' AND c.size_bytes>32768);

DROP TABLE provider_content_settlements;
