CREATE OR REPLACE PROCEDURE generate_transaction(i integer)
LANGUAGE plpgsql
AS $BODY$
    BEGIN
        FOR j in 0..i-1 LOOP
            INSERT INTO "transaction_logs" ("user_id","callback_id","reference","type","ref","data","comment","tenant","amount")
                VALUES ('1','qwerty12345','10101','go','heloshka','some meta info','some comment info','tur za tushur','1');
            UPDATE "users"
                SET "email" = 'test@example.com', "nickname" = 'youruniquenickname', "amount" = "amount" + 1
            WHERE "users"."id" = '1';
            COMMIT;
        END LOOP;
    END;
$BODY$;