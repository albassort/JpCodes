#!/bin/bash
PGPASSWORD='JpCodes' psql -U jpcodes -d jpcodes < schema.sql
