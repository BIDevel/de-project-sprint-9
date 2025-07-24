do $$
declare v_rec record;
        v_cur CURSOR for 
    select table_schema, table_name 
    from (
        values
        ('cdm', 'user_product_counters'),
        ('cdm', 'user_category_counters'),
        ('stg', 'order_events'),
        ('dds', 'h_user'),
        ('dds', 'h_product'),
        ('dds', 'h_category'),
        ('dds', 'h_restaurant'),
        ('dds', 'h_order'),
        ('dds', 'l_order_product'),
        ('dds', 'l_product_restaurant'),
        ('dds', 'l_product_category'),
        ('dds', 'l_order_user'),
        ('dds', 's_user_names'),
        ('dds', 's_product_names'),
        ('dds', 's_restaurant_names'),
        ('dds', 's_order_cost'),
        ('dds', 's_order_status') ) as tab (table_schema, table_name);
BEGIN
    open v_cur;
    loop
        fetch next from v_cur into v_rec;
        exit when not found;
        if exists (select 1 from information_schema.tables where table_schema = v_rec.table_schema and table_name = v_rec.table_name) then
            execute( concat('truncate table ', v_rec.table_schema, '.', v_rec.table_name, ' cascade;') );
            raise notice 'Таблица %.% очищена!', v_rec.table_schema, v_rec.table_name;
        else 
            raise notice 'Таблица %.% НЕ НАЙДЕНА!', v_rec.table_schema, v_rec.table_name;
        end if; 
   end loop;
   close v_cur;
END $$ language plpgsql;
