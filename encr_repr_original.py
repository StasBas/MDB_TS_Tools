#!/usr/bin/python3
import pymongo
from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.encryption import (Algorithm,
                                ClientEncryption)
from bson.binary import STANDARD, UUID
from bson.codec_options import CodecOptions
import base64


def insert_patient(collection, name, ssn, blood_type, medical_records, policy_number, provider):
    insurance = {
        'policyNumber': policy_number,
        'provider': provider
    }
    doc = {
        'name': name,
        'ssn': ssn,
        'bloodType': blood_type,
        'medicalRecords': medical_records,
        'insurance': insurance
    }
    collection.insert_one(doc)


def json_schema_creator(key_id):
    return {
        'bsonType': 'object',
        'encryptMetadata': {
            'keyId': key_id
        },
        'properties': {
            'insurance': {
                'bsonType': "object",
                'properties': {
                    'policyNumber': {
                        'encrypt': {
                            'bsonType': "int",
                            'algorithm':
                                "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
                        }
                    }
                }
            },
            'medicalRecords': {
                'encrypt': {
                    'bsonType': "array",
                    'algorithm': "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                }
            },
            'bloodType': {
                'encrypt': {
                    'bsonType': "string",
                    'algorithm': "AEAD_AES_256_CBC_HMAC_SHA_512-Random"
                }
            },
            'ssn': {
                'encrypt': {
                    'bsonType': 'int',
                    'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
                }
            },
            'mobile': {
                'encrypt': {
                    'bsonType': 'string',
                    'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Random',
                }
            }
        }
    }


connection_string = "mongodb://127.0.0.1:27017"
key_vault_namespace = "encryption.__keyVault"
kms_providers = {
    "aws": {
        "accessKeyId": "AKIA4GIVQVME2LQ5DMGG",
        "secretAccessKey": "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"
    }
}
fle_opts = AutoEncryptionOpts(kms_providers, key_vault_namespace, )
client = MongoClient(connection_string, auto_encryption_opts=fle_opts)
client_encryption = ClientEncryption(
    kms_providers,
    key_vault_namespace,
    client,
    CodecOptions(uuid_representation=STANDARD)
)
data_key_id = client_encryption.create_data_key(kms_provider="aws",
                                                master_key={
                                                    "region": "eu-west-1",
                                                    "key": "arn:aws:kms:eu-west-1:838100626185:key/24a31b92-7bfd-4a"
                                                           "e2-a0c9-e77c97db6126"})
uuid_data_key_id = UUID(bytes=data_key_id)
base_64_data_key_id = base64.b64encode(data_key_id)
print("DataKeyId [UUID]: ", str(uuid_data_key_id))
print("DataKeyId [base64]: ", base_64_data_key_id)
"""
db.getSiblingDB("test").runCommand(
  {
    "collMod" : "encoll",
    "validator" : {
      "$jsonSchema" : {
  "bsonType": "object",
  "encryptMetadata": {
    "keyId": [
      UUID("b1d7db1b-8663-411f-bf3f-a1af263f79dd") 
    ]
  },
  "properties": {
    "insurance": {
      "bsonType": "object",
      "properties": {
        "policyNumber": {
          "encrypt": {
            "bsonType": "int",
            "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
          }
        }
      }
    },
    "medicalRecords": {
      "encrypt": {
        "bsonType": "array",
        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      }
    },
    "bloodType": {
      "encrypt": {
        "bsonType": "string",
        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      }
    },
    "ssn": {
      "encrypt": {
        "bsonType": "int",
        "algorithm": "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic"
      }
    }
  }
}
    }
  }
);
"""
db = client['hr']
collection = db['employees']
insert_patient(collection, "shalom", 123456, "red", ["aaaa"], 123456, "shalom")
