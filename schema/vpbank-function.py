from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
import json

# app = FastAPI()

client = MongoClient('mongodb://admin:admin@localhost:27017/')
db = client['financial_db']

# --- Pydantic Models ---
class Address(BaseModel):
    street: str
    city: str
    country: str

class PersonalInfo(BaseModel):
    name: str
    email: str
    phone: str
    address: Address
    registration_date: datetime
    customer_since_months: int
    customer_type: str
    kyc_status: str

class Account(BaseModel):
    _id: int
    account_type: str
    account_number: str
    balance: float
    currency: str
    interest_rate: Optional[float] = None
    min_balance: Optional[float] = None
    overdraft_limit: Optional[float] = None
    status: str
    date_opened: Optional[datetime] = None

class Card(BaseModel):
    _id: int
    card_type: str
    masked_number: str
    linked_account: int
    credit_limit: float
    outstanding_balance: float
    available_credit: float
    monthly_fee: Optional[float] = None
    interest_rate: Optional[float] = None
    expiration_date: Optional[datetime] = None

class Loan(BaseModel):
    _id: int
    loan_type: str
    original_amount: float
    remaining_balance: float
    interest_rate: float
    monthly_payment: float
    next_payment_date: datetime
    term_remaining_months: int
    payment_history: str

class Transaction(BaseModel):
    _id: str
    account_id: int
    date: datetime
    amount: float
    type: str
    payment_method: str

class Analytics(BaseModel):
    _id: str
    period: str
    transaction_analytics: Dict[str, Any]
    financial_health_score: Dict[str, Any]
    last_updated: datetime

class CustomerResponse(BaseModel):
    _id: str
    personal_info: PersonalInfo
    accounts: List[Account]
    cards: List[Card]
    loans: List[Loan]
    transactions: List[Transaction]
    analytics: Optional[Analytics] = None
    primary_branch: str

# --- API Endpoint ---
# @app.get("/customer/{customer_id}", response_model=CustomerResponse)
def get_customer_data(customer_id: str):
    pipeline = [
        {"$match": {"_id": customer_id}},
        {"$lookup": {
            "from": "accounts",
            "localField": "accounts",
            "foreignField": "_id",
            "as": "accounts"
        }},
        {"$lookup": {
            "from": "cards",
            "localField": "cards",
            "foreignField": "_id",
            "as": "cards"
        }},
        {"$lookup": {
            "from": "loans",
            "localField": "loans",
            "foreignField": "_id",
            "as": "loans"
        }},
        {"$lookup": {
            "from": "transactions",
            "let": {"customer_id": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$customer_id", "$$customer_id"]}}}
            ],
            "as": "transactions"
        }},
        {"$lookup": {
            "from": "analytics",
            "let": {"customer_id": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$customer_id", "$$customer_id"]}}},
                {"$sort": {"last_updated": -1}},
                {"$limit": 1}
            ],
            "as": "analytics"
        }},
        {"$addFields": {
            "analytics": {"$arrayElemAt": ["$analytics", 0]}
        }}
    ]
    result = list(db.customers.aggregate(pipeline))
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found")
    customer = result[0]
    # Convert ObjectId and datetime to JSON serializable
    def convert(obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, list):
            return [convert(i) for i in obj]
        if isinstance(obj, dict):
            return {k: convert(v) for k, v in obj.items()}
        return obj
    customer = convert(customer)
    # print(customer)
    # return JSONResponse(content=jsonable_encoder(customer))
    y = json.dumps(customer)
    print(y)


if __name__ == '__main__':
    get_customer_data("CUST_12345")