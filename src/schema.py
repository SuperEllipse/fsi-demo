tbl_schemas = { 1: 
                {
                    "table": "trans",
                    "path": "data/trans.csv", 
                    "schema": """trans_id INT,
                                account_id INT,
                                `date` STRING,
                                type VARCHAR(20),
                                operation VARCHAR(20),
                                amount INT,
                                balance INT,
                                k_symbol VARCHAR(20),
                                bank VARCHAR(20),
                                account INT)"""
                },
            2:
                {
                    "table": "loan",
                    "path": "data/loan.csv", 
                    "schema": """
                                loan_id INT,
                                account_id INT,
                                `date` STRING,
                                amount INT,
                                duration INT,
                                payments DECIMAL(20, 2),
                                status VARCHAR(10)
                                """
                },
            3:
                {
                    "table": "account",
                    "path": "data/account.csv", 
                    "schema": """
                                account_id INT,
                                district_id INT,
                                frequency VARCHAR(20),
                                datecreated STRING
                                """
                },
            4:
                {
                    "table": "card",
                    "path": "data/card.csv", 
                    "schema": """
                                card_id INT,
                                disp_id INT,
                                type VARCHAR(20),
                                issued STRING
                                """       
                },
            5:
                {
                    "table": "client",
                    "path": "data/client.csv", 
                    "schema": """
                                client_id INT,
                                gender VARCHAR(10),
                                birth_date STRING,
                                district_id INT
                                """       
                },            
            6:
                {
                    "table": "orders",
                    "path": "data/order.csv", 
                    "schema": """
                                order_id INT,
                                account_id INT,
                                bank_to VARCHAR(5),
                                account_to INT,
                                amount DECIMAL(20, 2),
                                k_symbol VARCHAR(20)
                                """       
                },            
            7:
                {
                    "table": "disposition",
                    "path": "data/disp.csv", 
                    "schema": """
                                disp_id INT,
                                client_id INT,
                                account_id INT,
                                type VARCHAR(20)
                                """       
                },        
            8:
                {
                    "table": "district",
                    "path": "data/district.csv", 
                    "schema": """
                            district_id INT,
                            A2 VARCHAR(20),
                            A3 VARCHAR(20),
                            A4 INT,
                            A5 INT,
                            A6 INT,
                            A7 INT,
                            A8 INT,
                            A9 INT,
                            A10 DECIMAL(10, 1),
                            A11 INT,
                            A12 DECIMAL(2, 2),
                            A13 DECIMAL(2, 2),
                            A14 INT,
                            A15 INT,
                            A16 INT
                                """       
                }            
                
            }
