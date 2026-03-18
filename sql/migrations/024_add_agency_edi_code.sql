-- Add EDI code field to agencies (alphanumeric, up to 10 chars)
ALTER TABLE agencies ADD COLUMN edi_code TEXT;
