#!/usr/bin/python3
import pymongo
from pymongo import MongoClient
from pymongo.encryption_options import AutoEncryptionOpts
from pymongo.encryption import (Algorithm,
                                ClientEncryption)
from bson.binary import STANDARD, UUID
from bson.codec_options import CodecOptions
import base64
from random import randint, choice


CONN_STR = "mongodb://localhost:6070,localhost:6071,localhost6072/?replicaSet=rsname"
DB_NAME = "testEnc"
COLL_NAME = "records"


def main():
    key_vault_namespace = "encryption.__keyVault"
    kms_providers = {
        "aws": {
            "accessKeyId": "AKIA4GIVQVME2LQ5DMGG",
            "secretAccessKey": "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"
        }
    }
    fle_opts = AutoEncryptionOpts(kms_providers, key_vault_namespace, )
    client = MongoClient(CONN_STR, auto_encryption_opts=fle_opts)

    ##################################################################################################################
    # DEJUKE
    ##################################################################################################################

    client_encryption = ClientEncryption(
        kms_providers,
        key_vault_namespace,
        client,
        CodecOptions(uuid_representation=STANDARD)
    )
    data_key_id = client_encryption.create_data_key(
        kms_provider="aws",
        master_key={"region": "eu-west-1",
                    "key": "arn:aws:kms:eu-west-1:838100626185:key/24a31b92-7bfd-4ae2-a0c9-e77c97db6126"})
    uuid_data_key_id = UUID(bytes=data_key_id)
    base_64_data_key_id = base64.b64encode(data_key_id)
    print("DataKeyId [UUID]: ", str(uuid_data_key_id))
    print("DataKeyId [base64]: ", base_64_data_key_id)
    ##################################################################################################################

    db = client[DB_NAME]
    collection = db[COLL_NAME]

    for i in range(20):
        insert_patient(collection, f"patient_{i}", randint(100000, 999999),
                       choice(["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]),
                       [f"record_{j}" for j in range(-1, i)], randint(1, 9), choice(["p1", "p2", "p3"]))


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
    print(f"Inserting patient {name}")
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
                    'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
                }
            },
            'bloodType': {
                'encrypt': {
                    'bsonType': "string",
                    'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic'
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
                    'algorithm': 'AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic',
                }
            }
        }
    }


if __name__ == "__main__":
    main()
