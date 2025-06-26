#!/bin/bash
mongodump --uri="mongodb://admin:admin@mongodb:27017/financial_db?authSource=admin" --out=/dump
