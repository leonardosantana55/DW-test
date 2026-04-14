CREATE OR REPLACE FUNCTION column_names(t_name text)
RETURNS TABLE(name text, type text)
LANGUAGE sql
AS $$
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_name = t_name;
$$;
