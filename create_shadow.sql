create function create_shadow(pi_table character varying) returns json
    language plpgsql
as
$$
DECLARE
    v_schema_name character varying;
    v_table_name character varying;
    v_count integer;
    v_shadow_schema_name character varying;
    v_script character varying;
    v_shadow_schema_created character varying;
    v_shadow_table_name character varying;
    v_shadow_table_columns character varying;
    v_table_columns character varying :='';
    v_table_columns_names character varying :='';
    v_r information_schema.columns%rowtype;
    v_r_text text;
    v_col_name character varying;
    v_col_type character varying;
    v_is_nullable character varying;
    v_numeric_scale character varying;
    v_numeric_precision character varying;
    v_character_maximum_length character varying;
    v_sql_pk_key character varying;
    v_sql_sq character varying;
    v_tr_ins_fun_name character varying;
    v_sql_tr_ins_fun character varying;
    v_tr_new_columns_names character varying := '';
    v_sql_tr_insert character varying;
    v_body character varying;
    v_tr_upd_fun_name character varying;
    v_sql_tr_upd_fun character varying;
    v_sql_tr_update character varying;
    v_tr_del_fun_name character varying;
    v_sql_tr_del_fun character varying;
    v_tr_old_columns_names character varying := '';
    v_sql_tr_delete character varying;

BEGIN
    if strpos(pi_table,'.')<=0 then
        RETURN json_build_object('result',false,'err','schemaname.tablename');
    end if;
    select count(*) into v_count from pg_catalog.pg_tables t where pi_table = t.schemaname || '.' || t.tablename;
    if (v_count != 1) then
        RETURN json_build_object('result',false,'err','table not exist');
    end if;
    v_schema_name := split_part(pi_table, '.', 1);
    v_table_name := split_part(pi_table, '.', 2);
    v_shadow_schema_name = 'sha_' || v_schema_name;
    SELECT count(*) into v_count FROM information_schema.schemata t where t.schema_name = v_shadow_schema_name;
    v_shadow_schema_created = 'NO';
    if (v_count = 0) then
        EXECUTE 'CREATE SCHEMA IF NOT EXISTS ' || v_shadow_schema_name;
        v_shadow_schema_created = 'YES';
    end if;
    v_shadow_table_name = v_table_name || '_sha';
    select count(*) into v_count from pg_catalog.pg_tables t where v_shadow_schema_name || '.' || v_shadow_table_name = t.schemaname || '.' || t.tablename;
    if (v_count = 0) then
        v_shadow_table_columns =
                    v_shadow_table_name || '_key numeric NOT NULL,
		dtc timestamp without time zone NOT NULL,
    	op character varying(1) NOT NULL';
        FOR v_r IN
            SELECT column_name, data_type, is_nullable, numeric_precision, numeric_scale, character_maximum_length
            FROM information_schema.columns
            WHERE table_schema || '.' || table_name = pi_table
            LOOP
                v_r_text := REGEXP_REPLACE(v_r::text, '\(', '');
                v_r_text := REGEXP_REPLACE(v_r_text, '\)', '');
                v_r_text := REGEXP_REPLACE(v_r_text, '\s+$', '');
                --RAISE NOTICE 'row: %', v_r_text;
                v_col_name = split_part(v_r_text, ',', 1);
                v_col_type = split_part(v_r_text, ',', 2);
                v_is_nullable = split_part(v_r_text, ',', 3);
                if (v_is_nullable = 'NO') then
                    v_is_nullable:='NOT NULL';
                else
                    v_is_nullable:='';
                end if;
                if (v_col_type = 'numeric') then
                    v_numeric_precision = split_part(v_r_text, ',', 4);
                    v_numeric_scale = split_part(v_r_text, ',', 5);
                    --RAISE NOTICE 'v_numeric_precision: %', v_numeric_precision;
                    --RAISE NOTICE 'v_numeric_scale: %', v_numeric_scale;
                    if (v_numeric_precision != '') then
                        --RAISE NOTICE 'v_numeric_precision: %', 'null';
                        if (v_numeric_scale != '' and v_numeric_scale != '0') then
                            v_col_type := v_col_type || format('(%s,%s)', v_numeric_precision, v_numeric_scale);
                            --RAISE NOTICE 'v_col_type: %', v_col_type;
                        else
                            v_col_type := v_col_type || format('(%s,0)', v_numeric_precision);
                            --RAISE NOTICE 'v_col_type: %', v_col_type;
                        end if;
                    end if;

                end if;
                if (v_col_type='"character varying"') then
                    v_col_type:='character varying';
                    --RAISE NOTICE 'v_col_type: %', v_col_type;
                    v_character_maximum_length = split_part(v_r_text, ',', 6);
                    if (v_character_maximum_length != '') then
                        v_col_type := v_col_type || format('(%s)', v_character_maximum_length);
                    end if;

                end if;
                --RAISE NOTICE 'v_col_type: %', v_col_type;
                --RAISE NOTICE 'v_is_nullable:%', v_is_nullable;
                v_table_columns = v_table_columns ||', ' || v_col_name || ' ' || v_col_type || ' ' || v_is_nullable;
                v_table_columns_names = v_table_columns_names || ', ' || v_col_name;
                v_tr_new_columns_names = v_tr_new_columns_names || ', ' || 'NEW.' || v_col_name;
                v_tr_old_columns_names = v_tr_old_columns_names || ', ' || 'OLD.' || v_col_name;
                --RAISE NOTICE 'v_table_columns: %', v_table_columns;
                --RETURN NEXT v_r; -- возвращается текущая строка запроса
            END LOOP;
        --RAISE NOTICE 'v_table_columns: %', v_table_columns;
        v_sql_pk_key = format (',CONSTRAINT %s PRIMARY KEY (%s)','pk_' || v_shadow_table_name || '_key', v_shadow_table_name || '_key');
        EXECUTE 'CREATE TABLE IF NOT EXISTS '|| v_shadow_schema_name || '.' || v_shadow_table_name || '(' || v_shadow_table_columns || v_table_columns || v_sql_pk_key || ')';
        v_sql_sq = format('CREATE SEQUENCE IF NOT EXISTS %s.%s INCREMENT 1 START 1000 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1', v_shadow_schema_name, 'sq_' || v_shadow_table_name);
        EXECUTE v_sql_sq;
        --CREATE INS TRIGER FUNCTION
        v_tr_ins_fun_name = 'ins_' || v_table_name;
        v_sql_tr_ins_fun = format('CREATE OR REPLACE FUNCTION %s.%s()', v_schema_name, v_tr_ins_fun_name);
        v_body = '%sBODY%s';
        v_body = format(v_body, '$', '$');
        v_sql_tr_ins_fun = v_sql_tr_ins_fun || ' RETURNS trigger LANGUAGE ''plpgsql'' COST 100 VOLATILE NOT LEAKPROOF AS ' || v_body || ' %s';
        v_sql_tr_ins_fun = format(v_sql_tr_ins_fun, ' DECLARE key_val numeric; BEGIN %s  END;');
        v_sql_tr_ins_fun = format(v_sql_tr_ins_fun, 'key_val := nextval(''%s.%s'');%s RETURN NEW;');
        v_sql_tr_ins_fun = format(v_sql_tr_ins_fun, v_shadow_schema_name, 'sq_' || v_shadow_table_name, '%s' );
        v_sql_tr_ins_fun = format(v_sql_tr_ins_fun, '		INSERT INTO %s.%s%s;');
        v_sql_tr_ins_fun = format(v_sql_tr_ins_fun, v_shadow_schema_name, v_shadow_table_name, '%s');
        v_sql_tr_ins_fun = format(v_sql_tr_ins_fun, '(t3_sha_key, dtc, op %s) VALUES (key_val, now(), ''i'' %s)');
        v_sql_tr_ins_fun = format(v_sql_tr_ins_fun, v_table_columns_names, v_tr_new_columns_names);
        v_sql_tr_ins_fun = v_sql_tr_ins_fun || ' ' || v_body || ';';
        --RAISE NOTICE 'v_sql_tr_ins_fun: %', v_sql_tr_ins_fun;
        EXECUTE v_sql_tr_ins_fun;
        --CREATE TRIGGER BEFORE INSERT
        v_sql_tr_insert = format('CREATE TRIGGER sha_ins BEFORE INSERT ON %s.%s FOR EACH ROW EXECUTE FUNCTION %s.%s();', v_schema_name, v_table_name, v_schema_name, v_tr_ins_fun_name);
        --RAISE NOTICE 'v_sql_tr_insert: %', v_sql_tr_insert;
        EXECUTE v_sql_tr_insert;
        --CREATE UPD TRIGER FUNCTION
        v_tr_upd_fun_name = 'upd_' || v_table_name;
        v_sql_tr_upd_fun = format('CREATE OR REPLACE FUNCTION %s.%s()', v_schema_name, v_tr_upd_fun_name);
        v_body = '%sBODY%s';
        v_body = format(v_body, '$', '$');
        v_sql_tr_upd_fun = v_sql_tr_upd_fun || ' RETURNS trigger LANGUAGE ''plpgsql'' COST 100 VOLATILE NOT LEAKPROOF AS ' || v_body || ' %s';
        v_sql_tr_upd_fun = format(v_sql_tr_upd_fun, ' DECLARE key_val numeric; BEGIN %s  END;');
        v_sql_tr_upd_fun = format(v_sql_tr_upd_fun, 'key_val := nextval(''%s.%s'');%s RETURN NEW;');
        v_sql_tr_upd_fun = format(v_sql_tr_upd_fun, v_shadow_schema_name, 'sq_' || v_shadow_table_name, '%s' );
        v_sql_tr_upd_fun = format(v_sql_tr_upd_fun, '		INSERT INTO %s.%s%s;');
        v_sql_tr_upd_fun = format(v_sql_tr_upd_fun, v_shadow_schema_name, v_shadow_table_name, '%s');
        v_sql_tr_upd_fun = format(v_sql_tr_upd_fun, '(t3_sha_key, dtc, op %s) VALUES (key_val, now(), ''u'' %s)');
        v_sql_tr_upd_fun = format(v_sql_tr_upd_fun, v_table_columns_names, v_tr_new_columns_names);
        v_sql_tr_upd_fun = v_sql_tr_upd_fun || ' ' || v_body || ';';
        --RAISE NOTICE 'v_sql_tr_upd_fun: %', v_sql_tr_upd_fun;
        EXECUTE v_sql_tr_upd_fun;
        --CREATE TRIGGER BEFORE UPDATE
        v_sql_tr_update = format('CREATE TRIGGER sha_upd BEFORE UPDATE ON %s.%s FOR EACH ROW EXECUTE FUNCTION %s.%s();', v_schema_name, v_table_name, v_schema_name, v_tr_upd_fun_name);
        --RAISE NOTICE 'v_sql_tr_update: %', v_sql_tr_update;
        EXECUTE v_sql_tr_update;
        --CREATE DEL TRIGER FUNCTION
        v_tr_del_fun_name = 'del_' || v_table_name;
        v_sql_tr_del_fun = format('CREATE OR REPLACE FUNCTION %s.%s()', v_schema_name, v_tr_del_fun_name);
        v_body = '%sBODY%s';
        v_body = format(v_body, '$', '$');
        v_sql_tr_del_fun = v_sql_tr_del_fun || ' RETURNS trigger LANGUAGE ''plpgsql'' COST 100 VOLATILE NOT LEAKPROOF AS ' || v_body || ' %s';
        v_sql_tr_del_fun = format(v_sql_tr_del_fun, ' DECLARE key_val numeric; BEGIN %s  END;');
        v_sql_tr_del_fun = format(v_sql_tr_del_fun, 'key_val := nextval(''%s.%s'');%s RETURN OLD;');
        v_sql_tr_del_fun = format(v_sql_tr_del_fun, v_shadow_schema_name, 'sq_' || v_shadow_table_name, '%s' );
        v_sql_tr_del_fun = format(v_sql_tr_del_fun, '		INSERT INTO %s.%s%s;');
        v_sql_tr_del_fun = format(v_sql_tr_del_fun, v_shadow_schema_name, v_shadow_table_name, '%s');
        v_sql_tr_del_fun = format(v_sql_tr_del_fun, '(t3_sha_key, dtc, op %s) VALUES (key_val, now(), ''d'' %s)');
        v_sql_tr_del_fun = format(v_sql_tr_del_fun, v_table_columns_names, v_tr_old_columns_names);
        v_sql_tr_del_fun = v_sql_tr_del_fun || ' ' || v_body || ';';
        EXECUTE v_sql_tr_del_fun;
        --CREATE TRIGGER BEFORE DELETE
        v_sql_tr_delete = format('CREATE TRIGGER sha_del BEFORE DELETE ON %s.%s FOR EACH ROW EXECUTE FUNCTION %s.%s();', v_schema_name, v_table_name, v_schema_name, v_tr_del_fun_name);
        EXECUTE v_sql_tr_delete;

    end if;
    RETURN json_build_object('result',true,'schemaname',v_schema_name,
                             'v_tablename',v_table_name,'shadow_schema', v_shadow_schema_name,
                             'shadow_schema_created', v_shadow_schema_created);
END;
$$;

alter function create_shadow(varchar) owner to postgres;

