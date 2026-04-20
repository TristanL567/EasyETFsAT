-- 1) Table existence check (source + curated core)
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
    'SECMDA', 'SECDIV',
    'SOURCERPT', 'SOURCEAGE', 'SOURCERAW',
    'TAXRPT', 'TAXDAT', 'TAXADJ', 'TAXLIN', 'TAXCAT', 'TAXCOR',
    'REFCCY', 'REFCTR', 'REFEXC', 'IMPLOG', 'IMPERR'
  )
ORDER BY table_name;

-- 1b) View existence check
SELECT table_name
FROM information_schema.views
WHERE table_schema = 'public'
  AND table_name IN ('V1_TAXDATPRE', 'V2_TAXDATEUR')
ORDER BY table_name;

-- 2) Unique constraints for business keys
SELECT
  tc.table_name,
  tc.constraint_name,
  string_agg(kcu.column_name, ', ' ORDER BY kcu.ordinal_position) AS columns
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
  ON tc.constraint_name = kcu.constraint_name
  AND tc.table_schema = kcu.table_schema
WHERE tc.table_schema = 'public'
  AND tc.constraint_type = 'UNIQUE'
  AND tc.table_name IN ('SECMDA', 'SOURCERPT', 'SOURCEAGE', 'SOURCERAW', 'TAXRPT', 'TAXDAT', 'TAXADJ', 'REFEXC')
GROUP BY tc.table_name, tc.constraint_name
ORDER BY tc.table_name, tc.constraint_name;

-- 3) Upsert key check example (TAXRPT)
INSERT INTO "TAXRPT" (
  "TAXISN", "TAXOKBIDN", "TAXVRN", "TAXSTS", "TAXYEA", "TAXCCY"
) VALUES (
  'IE00BMTX1Y45', 99999999, 1, 'FIN', 2025, 'EUR'
)
ON CONFLICT ("TAXISN", "TAXOKBIDN")
DO UPDATE SET
  "TAXVRN" = EXCLUDED."TAXVRN",
  "TAXSTS" = EXCLUDED."TAXSTS",
  "TAXYEA" = EXCLUDED."TAXYEA",
  "TAXCCY" = EXCLUDED."TAXCCY",
  "TAXUPDDTS" = now()
WHERE EXCLUDED."TAXVRN" >= "TAXRPT"."TAXVRN";

SELECT "TAXISN", "TAXOKBIDN", "TAXVRN", "TAXSTS", "TAXYEA", "TAXCCY"
FROM "TAXRPT"
WHERE "TAXISN" = 'IE00BMTX1Y45' AND "TAXOKBIDN" = 99999999;
