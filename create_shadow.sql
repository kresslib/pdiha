-- FUNCTION: public.create_shadow(character varying)

-- DROP FUNCTION IF EXISTS public.create_shadow(character varying);

CREATE OR REPLACE FUNCTION public.create_shadow(
	pi_table character varying)
    RETURNS json
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    v_schemaname character varying;
	v_tablename character varying;
	v_count integer;
	v_shadow_schema character varying;
	v_script character varying;
	v_shadow_schema_created character varying;
	v_shadow_table character varying;
	v_shadow_table_columns character varying;
	v_table_columns character varying :='';
	v_r information_schema.columns%rowtype;
	v_r_text text;
	v_col_name character varying;
	v_col_type character varying;
	v_is_nullable character varying;
BEGIN
    if strpos(pi_table,'.')<=0 then
		RETURN json_build_object('result',false,'err','schemaname.tablename');
	end if;
    v_schemaname := split_part(pi_table, '.', 1);
	v_tablename := split_part(pi_table, '.', 2);
    -- SELECT schemaname, tablename, tableowner, tablespace, hasindexes, hasrules, hastriggers, rowsecurity
	-- FROM pg_catalog.pg_tables;
	select count(*) into v_count from pg_catalog.pg_tables t where pi_table = t.schemaname || '.' || t.tablename;
    if (v_count != 1) then
		RETURN json_build_object('result',false,'err','table not exist');
	end if;	
	v_shadow_schema = 'sha_' || v_schemaname;
	SELECT count(*) into v_count FROM information_schema.schemata t where t.schema_name = v_shadow_schema;
	v_shadow_schema_created = 'NO';
	if (v_count=0) then
	 	EXECUTE 'CREATE SCHEMA IF NOT EXISTS ' || v_shadow_schema;
		v_shadow_schema_created = 'YES';
	end if;
	v_shadow_table = v_tablename || '_sha';
	select count(*) into v_count from pg_catalog.pg_tables t where v_shadow_schema || '.' || v_shadow_table = t.schemaname || '.' || t.tablename;
	if (v_count=0) then
	    v_shadow_table_columns = 
		v_shadow_table || '_key numeric NOT NULL, 
		dtc timestamp without time zone NOT NULL,
    	op character varying(1) NOT NULL';
		FOR v_r IN
        SELECT column_name, data_type, is_nullable  
			FROM information_schema.columns 
			WHERE table_schema || '.' || table_name = pi_table
		LOOP
		    v_r_text := REGEXP_REPLACE(v_r::text, '\(', '');
			v_r_text := REGEXP_REPLACE(v_r_text, '\)', '');
			v_r_text := REGEXP_REPLACE(v_r_text, '\s+$', '');
			RAISE NOTICE 'row: %', v_r_text;
			v_col_name = split_part(v_r_text, ',', 1);
			v_col_type = split_part(v_r_text, ',', 2);
			v_is_nullable = split_part(v_r_text, ',', 3);
			if (v_is_nullable = 'NO') then
				v_is_nullable:='NOT NULL';
			else
				v_is_nullable:='';
			end if;
			RAISE NOTICE 'v_is_nullable:%', v_is_nullable;
			v_table_columns = v_table_columns ||', ' || v_col_name || ' ' || v_col_type || ' ' || v_is_nullable;
			--RAISE NOTICE 'v_table_columns: %', v_table_columns;
			--RETURN NEXT v_r; -- возвращается текущая строка запроса
		END LOOP;
		--RAISE NOTICE 'v_table_columns: %', v_table_columns;
		
		EXECUTE 'CREATE TABLE IF NOT EXISTS '|| v_shadow_schema || '.' || v_shadow_table || '(' || v_shadow_table_columns || v_table_columns || ')';
	end if;
	RETURN json_build_object('result',true,'schemaname',v_schemaname,
							 'v_tablename',v_tablename,'shadow_schema', v_shadow_schema,
							'shadow_schema_created', v_shadow_schema_created);
END;
$BODY$;

ALTER FUNCTION public.create_shadow(character varying)
    OWNER TO postgres;
