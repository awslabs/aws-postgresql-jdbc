#!/bin/bash
set -e

if [[ "${SCRAM}" == *"yes"* ]];
then
    PASSWORD_ENCRYPTION=scram-sha-256
else
    PASSWORD_ENCRYPTION=on
fi

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SET password_encryption='$PASSWORD_ENCRYPTION';
    CREATE USER test with password 'test';
    CREATE DATABASE test OWNER test;
    ALTER USER test with Replication;
    ALTER SYSTEM SET wal_level TO 'logical';
EOSQL

#/home/scripts/travis_ssl_users.sh
