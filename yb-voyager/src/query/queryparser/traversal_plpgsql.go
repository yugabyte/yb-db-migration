/*
Copyright (c) YugabyteDB, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package queryparser

import (
	"encoding/json"
	"fmt"
	"strings"

	log "github.com/sirupsen/logrus"
)

/*
*
This function is not concrete yet because of following limitation from parser -
The following queries are not the actual query we need so if we pass all these queries directly to parser again to detect the unsupported feature/construct. It will fail for some of these with syntax error, e.g.

			a. query "balance > 0 AND balance < withdrawal;" error - syntax error at or near "balance"
			b. query "format('
	            CREATE TABLE IF NOT EXISTS %I (
	                id SERIAL PRIMARY KEY,
	                name TEXT,
	                amount NUMERIC
	            )', partition_table);" error - syntax error at or near "format"
			c. query "(SELECT balance FROM accounts WHERE account_id = sender_id) < transfer_amount;" error - syntax error at or near "<"

These issues are majorly expressions, conditions, assignments, loop variables, raise parameters, etc… and the parser is giving all these as queries so we can’t differentiate  as such between actual query and these.
*
*/
func GetAllPLPGSQLStatements(query string) ([]string, error) {
	parsedJson, parsedJsonMap, err := getParsedJsonMap(query)
	if err != nil {
		return []string{}, err
	}

	function := parsedJsonMap[PLPGSQL_FUNCTION]
	parsedFunctionMap, ok := function.(map[string]interface{})
	if !ok {
		return []string{}, fmt.Errorf("the PlPgSQL_Function field is not a map in parsed json-%s", parsedJson)
	}

	actions := parsedFunctionMap[ACTION]
	var plPgSqlStatements []string
	TraversePlPgSQLJson(actions, &plPgSqlStatements)
	return plPgSqlStatements, nil
}

/*
Query example-

	CREATE OR REPLACE FUNCTION func_example_advi(
	   sender_id INT,
	   receiver_id INT,
	   transfer_amount NUMERIC

)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN

	-- Acquire advisory locks to prevent concurrent updates on the same accounts
	PERFORM pg_advisory_lock(sender_id);
	PERFORM pg_advisory_lock(receiver_id);

	-- Deduct the amount from the sender's account
	UPDATE accounts
	SET balance = balance - transfer_amount
	WHERE account_id = sender_id;

	-- Add the amount to the receiver's account
	UPDATE accounts
	SET balance = balance + transfer_amount
	WHERE account_id = receiver_id;

	-- Release the advisory locks
	PERFORM pg_advisory_unlock(sender_id);
	PERFORM pg_advisory_unlock(receiver_id);

END;
$$;

parsed json -

	{
	  "PLpgSQL_function": {
	    "datums": [
	      ...
	    ],
	    "action": {
	      "PLpgSQL_stmt_block": {
	        "lineno": 2,
	        "body": [
	          {
	            "PLpgSQL_stmt_perform": {
	              "lineno": 4,
	              "expr": {
	                "PLpgSQL_expr": {
	                  "query": "SELECT pg_advisory_lock(sender_id)",
	                  "parseMode": 0
	                }
	              }
	            }
	          },
	          {
				... similar to above
	          },
	          {
	            "PLpgSQL_stmt_execsql": {
	              "lineno": 8,
	              "sqlstmt": {
	                "PLpgSQL_expr": {
	                  "query": "UPDATE accounts \n    SET balance = balance - transfer_amount \n    WHERE account_id = sender_id",
	                  "parseMode": 0
	                }
	              }
	            }
	          },
	          {
				.... similar to above
	          },
	          {
	            ... similar to below
	          },
	          {
	            "PLpgSQL_stmt_perform": {
	              "lineno": 19,
	              "expr": {
	                "PLpgSQL_expr": {
	                  "query": "SELECT pg_advisory_unlock(receiver_id)",
	                  "parseMode": 0
	                }
	              }
	            }
	          },
	          {
	            "PLpgSQL_stmt_return": {}
	          }
	        ]
	      }
	    }
	  }
	}
*/
func TraversePlPgSQLJson(fieldValue interface{}, plPgSqlStatements *[]string) {
	fieldMap, isMap := fieldValue.(map[string]interface{})
	fieldList, isList := fieldValue.([]interface{})
	switch true {
	case isMap:
		for k, v := range fieldMap {
			switch k {
			// base case of recursive calls to reach this PLPGSQL_EXPR field in json which will have "query" field with statement
			case PLPGSQL_EXPR:
				expr, ok := v.(map[string]interface{})
				if ok {
					query, ok := expr[QUERY]
					if ok {
						q := formatExprQuery(query.(string)) // formating the query of parsed json if required

						*plPgSqlStatements = append(*plPgSqlStatements, q)
					}
				}
			default:
				TraversePlPgSQLJson(v, plPgSqlStatements)
			}
		}
	case isList:
		//In case the value of a field is not a <key , val> but a list of <key, val> e.g. "body"
		for _, l := range fieldList {
			TraversePlPgSQLJson(l, plPgSqlStatements)
		}
	}

}

// Function to format the PLPGSQL EXPR query from the json string
func formatExprQuery(q string) string {
	/*
		PLPGSQL line -
		EXECUTE 'DROP TABLE IF EXISTS employees';

		json str -
		 "PLpgSQL_expr": {
			"query": "'DROP TABLE IF EXISTS employees'",
	*/
	q = strings.Trim(q, "'") //to remove any extra '' around the statement
	q = strings.TrimSpace(q)
	if !strings.HasSuffix(q, ";") { // adding the ; to query in case not added already
		q += ";"
	}
	return q
}

func getParsedJsonMap(query string) (string, map[string]interface{}, error) {
	parsedJson, err := ParsePLPGSQLToJson(query)
	if err != nil {
		log.Infof("error in parsing the stmt-%s to json: %v", query, err)
		return parsedJson, nil, err
	}
	if parsedJson == "" {
		return "", nil, nil
	}
	var parsedJsonMapList []map[string]interface{}
	//Refer to the queryparser.traversal_plpgsql.go for example and sample parsed json
	log.Debugf("parsing the json string-%s of stmt-%s", parsedJson, query)
	err = json.Unmarshal([]byte(parsedJson), &parsedJsonMapList)
	if err != nil {
		return parsedJson, nil, fmt.Errorf("error parsing the json string of stmt-%s: %v", query, err)
	}

	if len(parsedJsonMapList) == 0 {
		return parsedJson, nil, nil
	}

	return parsedJson, parsedJsonMapList[0], nil
}

/*
example -
CREATE FUNCTION public.get_employeee_salary(emp_id employees.employee_id%TYPE) RETURNS employees.salary%Type

	LANGUAGE plpgsql
	AS $$

DECLARE

	emp_salary employees.salary%TYPE;

BEGIN

	SELECT salary INTO emp_salary
	FROM employees
	WHERE employee_id = emp_id;
	RETURN emp_salary;

END;
$$;
[

	    {
	        "PLpgSQL_function": {
	            "datums": [
	                {
	                    "PLpgSQL_var": {
	                        "refname": "emp_id",
	                        "datatype": {
	                            "PLpgSQL_type": {
	                                "typname": "UNKNOWN"
	                            }
	                        }
	                    }
	                },
	                {
	                    "PLpgSQL_var": {
	                        "refname": "found",
	                        "datatype": {
	                            "PLpgSQL_type": {
	                                "typname": "UNKNOWN"
	                            }
	                        }
	                    }
	                },
	                {
	                    "PLpgSQL_var": {
	                        "refname": "emp_salary",
	                        "lineno": 3,
	                        "datatype": {
	                            "PLpgSQL_type": {
	                                "typname": "employees.salary%TYPE"
	                            }
	                        }
	                    }
	                },
	                {
	                    "PLpgSQL_row": {
	                        "refname": "(unnamed row)",
	                        "lineno": 5,
	                        "fields": [
	                            {
	                                "name": "emp_salary",
	                                "varno": 2
	                            }
	                        ]
	                    }
	                }
	            ],"action": {
	               ....
	            }
	        }
	    },

		Caveats:
		1. Not returning typename for variables in function parameter from this function (in correct in json as UNKNOWN), for that using the GetTypeNamesFromFuncParameters()
		2. Not returning the return type from this function (not available in json), for that using the GetReturnTypeOfFunc()
*/
func GetAllTypeNamesInPlpgSQLStmt(query string) ([]string, error) {
	parsedJson, parsedJsonMap, err := getParsedJsonMap(query)
	if err != nil {
		return []string{}, nil
	}
	function := parsedJsonMap[PLPGSQL_FUNCTION]
	parsedFunctionMap, ok := function.(map[string]interface{})
	if !ok {
		return []string{}, fmt.Errorf("the PlPgSQL_Function field is not a map in parsed json-%s", parsedJson)
	}

	datums := parsedFunctionMap[DATUMS]
	datumList, isList := datums.([]interface{})
	if !isList {
		return []string{}, fmt.Errorf("type names datums field is not list in parsed json-%s", parsedJson)
	}

	var typeNames []string
	for _, datum := range datumList {
		datumMap, ok := datum.(map[string]interface{})
		if !ok {
			log.Errorf("datum is not a map-%v", datum)
			continue
		}
		for key, val := range datumMap {
			switch key {
			case PLPGSQL_VAR:
				typeName, err := getTypeNameFromPlpgSQLVar(val)
				if err != nil {
					log.Errorf("not able to get typename from PLPGSQL_VAR(%v): %v", val, err)
					continue
				}
				typeNames = append(typeNames, typeName)
			}
		}
	}
	return typeNames, nil
}

/*
example of PLPGSQL_VAR -

	"PLpgSQL_var": {
		"refname": "tax_rate",
		"lineno": 3,
		"datatype": {
			"PLpgSQL_type": {
				"typname": "employees.tax_rate%TYPE"
			}
		}
	}
*/
func getTypeNameFromPlpgSQLVar(plpgsqlVar interface{}) (string, error) {
	//getting the map of <key,val > of PLpgSQL_Var json
	valueMap, ok := plpgsqlVar.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("PLPGSQL_VAR is not a map-%v", plpgsqlVar)
	}

	//getting the "datatype" field of PLpgSQL_Var json
	datatype, ok := valueMap[DATATYPE]
	if !ok {
		return "", fmt.Errorf("datatype is not in the PLPGSQL_VAR map-%v", valueMap)
	}

	datatypeValueMap, ok := datatype.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("datatype is not a map-%v", datatype)
	}

	plpgsqlType, ok := datatypeValueMap[PLPGSQL_TYPE]
	if !ok {
		return "", fmt.Errorf("PLPGSQL_Type is not in the datatype map-%v", datatypeValueMap)
	}

	typeValueMap, ok := plpgsqlType.(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("PLPGSQL_Type is not a map-%v", plpgsqlType)
	}

	typeName, ok := typeValueMap[TYPENAME]
	if !ok {
		return "", fmt.Errorf("typname is not in the PLPGSQL_Type map-%v", typeValueMap)
	}

	return typeName.(string), nil

}
