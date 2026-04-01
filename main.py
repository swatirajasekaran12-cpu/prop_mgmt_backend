from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery

app = FastAPI()

PROJECT_ID = "mgmt-545"
DATASET = "property_mgmt"


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client()
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    properties = [dict(row) for row in results]
    return properties
# ---------------------------------------------------------------------------
# Get one property by ID
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property not found"
        )

    return dict(results[0])


# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------

@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    query = f"""
        SELECT
            income_id,
            property_id,
            amount,
            date,
            description
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        property_check = list(bq.query(check_query, job_config=job_config).result())
        if not property_check:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        results = bq.query(query, job_config=job_config).result()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.post("/income/{property_id}")
def create_income(property_id: int, payload: dict, bq: bigquery.Client = Depends(get_bq_client)):
    amount = payload.get("amount")
    date = payload.get("date")
    description = payload.get("description")

    if amount is None or date is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="amount and date are required"
        )

    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    next_id_query = f"""
        SELECT COALESCE(MAX(income_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.income`
    """

    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.income`
        (income_id, property_id, amount, date, description)
        VALUES (@income_id, @property_id, @amount, @date, @description)
    """

    try:
        property_check = list(
            bq.query(
                check_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
                    ]
                )
            ).result()
        )

        if not property_check:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        next_id_result = list(bq.query(next_id_query).result())
        next_id = next_id_result[0]["next_id"]

        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("income_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", amount),
                bigquery.ScalarQueryParameter("date", "DATE", date),
                bigquery.ScalarQueryParameter("description", "STRING", description),
            ]
        )

        bq.query(insert_query, job_config=insert_config).result()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

    return {
        "income_id": next_id,
        "property_id": property_id,
        "amount": amount,
        "date": date,
        "description": description
    }


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------

@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    query = f"""
        SELECT
            expense_id,
            property_id,
            amount,
            date,
            category,
            vendor,
            description
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        property_check = list(bq.query(check_query, job_config=job_config).result())
        if not property_check:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        results = bq.query(query, job_config=job_config).result()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row) for row in results]


@app.post("/expenses/{property_id}")
def create_expense(property_id: int, payload: dict, bq: bigquery.Client = Depends(get_bq_client)):
    amount = payload.get("amount")
    date = payload.get("date")
    category = payload.get("category")
    vendor = payload.get("vendor")
    description = payload.get("description")

    if amount is None or date is None or category is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="amount, date, and category are required"
        )

    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    next_id_query = f"""
        SELECT COALESCE(MAX(expense_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.expenses`
    """

    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.expenses`
        (expense_id, property_id, amount, date, category, vendor, description)
        VALUES (@expense_id, @property_id, @amount, @date, @category, @vendor, @description)
    """

    try:
        property_check = list(
            bq.query(
                check_query,
                job_config=bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
                    ]
                )
            ).result()
        )

        if not property_check:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        next_id_result = list(bq.query(next_id_query).result())
        next_id = next_id_result[0]["next_id"]

        insert_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("expense_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
                bigquery.ScalarQueryParameter("amount", "FLOAT64", amount),
                bigquery.ScalarQueryParameter("date", "DATE", date),
                bigquery.ScalarQueryParameter("category", "STRING", category),
                bigquery.ScalarQueryParameter("vendor", "STRING", vendor),
                bigquery.ScalarQueryParameter("description", "STRING", description),
            ]
        )

        bq.query(insert_query, job_config=insert_config).result()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

    return {
        "expense_id": next_id,
        "property_id": property_id,
        "amount": amount,
        "date": date,
        "category": category,
        "vendor": vendor,
        "description": description
    }


# ---------------------------------------------------------------------------
# Additional endpoint 1: property summary
# ---------------------------------------------------------------------------

@app.get("/properties/{property_id}/summary")
def get_property_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            p.property_id,
            p.name,
            COALESCE(i.total_income, 0) AS total_income,
            COALESCE(e.total_expenses, 0) AS total_expenses,
            COALESCE(i.total_income, 0) - COALESCE(e.total_expenses, 0) AS profit
        FROM `{PROJECT_ID}.{DATASET}.properties` p
        LEFT JOIN (
            SELECT property_id, SUM(amount) AS total_income
            FROM `{PROJECT_ID}.{DATASET}.income`
            GROUP BY property_id
        ) i
        ON p.property_id = i.property_id
        LEFT JOIN (
            SELECT property_id, SUM(amount) AS total_expenses
            FROM `{PROJECT_ID}.{DATASET}.expenses`
            GROUP BY property_id
        ) e
        ON p.property_id = e.property_id
        WHERE p.property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Property not found"
        )

    return dict(results[0])
@app.post("/properties")
def create_property(payload: dict, bq: bigquery.Client = Depends(get_bq_client)):
    name = payload.get("name")
    address = payload.get("address")
    city = payload.get("city")
    state = payload.get("state")
    postal_code = payload.get("postal_code")
    property_type = payload.get("property_type")
    tenant_name = payload.get("tenant_name")
    monthly_rent = payload.get("monthly_rent")

    if not all([name, address, city, state, postal_code, property_type]):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="name, address, city, state, postal_code, and property_type are required"
        )

    next_id_query = f"""
        SELECT COALESCE(MAX(property_id), 0) + 1 AS next_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
    """

    insert_query = f"""
        INSERT INTO `{PROJECT_ID}.{DATASET}.properties`
        (property_id, name, address, city, state, postal_code, property_type, tenant_name, monthly_rent)
        VALUES (@property_id, @name, @address, @city, @state, @postal_code, @property_type, @tenant_name, @monthly_rent)
    """

    try:
        next_id_result = list(bq.query(next_id_query).result())
        next_id = next_id_result[0]["next_id"]

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", next_id),
                bigquery.ScalarQueryParameter("name", "STRING", name),
                bigquery.ScalarQueryParameter("address", "STRING", address),
                bigquery.ScalarQueryParameter("city", "STRING", city),
                bigquery.ScalarQueryParameter("state", "STRING", state),
                bigquery.ScalarQueryParameter("postal_code", "STRING", postal_code),
                bigquery.ScalarQueryParameter("property_type", "STRING", property_type),
                bigquery.ScalarQueryParameter("tenant_name", "STRING", tenant_name),
                bigquery.ScalarQueryParameter("monthly_rent", "FLOAT64", monthly_rent),
            ]
        )

        bq.query(insert_query, job_config=job_config).result()

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database insert failed: {str(e)}"
        )

    return {
        "property_id": next_id,
        "name": name,
        "address": address,
        "city": city,
        "state": state,
        "postal_code": postal_code,
        "property_type": property_type,
        "tenant_name": tenant_name,
        "monthly_rent": monthly_rent
    }


@app.put("/properties/{property_id}")
def update_property(property_id: int, payload: dict, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    try:
        check_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
            ]
        )
        existing = list(bq.query(check_query, job_config=check_config).result())

        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        allowed_fields = [
            "name", "address", "city", "state",
            "postal_code", "property_type", "tenant_name", "monthly_rent"
        ]

        updates = []
        params = [
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]

        for field in allowed_fields:
            if field in payload and payload[field] is not None:
                updates.append(f"{field} = @{field}")
                if field == "monthly_rent":
                    params.append(bigquery.ScalarQueryParameter(field, "FLOAT64", payload[field]))
                else:
                    params.append(bigquery.ScalarQueryParameter(field, "STRING", payload[field]))

        if not updates:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields provided for update"
            )

        update_query = f"""
            UPDATE `{PROJECT_ID}.{DATASET}.properties`
            SET {", ".join(updates)}
            WHERE property_id = @property_id
        """

        update_config = bigquery.QueryJobConfig(query_parameters=params)
        bq.query(update_query, job_config=update_config).result()

        result_query = f"""
            SELECT *
            FROM `{PROJECT_ID}.{DATASET}.properties`
            WHERE property_id = @property_id
        """
        updated = list(bq.query(result_query, job_config=check_config).result())

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database update failed: {str(e)}"
        )

    return dict(updated[0])


@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    check_query = f"""
        SELECT property_id
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    delete_income_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
    """

    delete_expenses_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
    """

    delete_property_query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        existing = list(bq.query(check_query, job_config=job_config).result())

        if not existing:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Property not found"
            )

        bq.query(delete_income_query, job_config=job_config).result()
        bq.query(delete_expenses_query, job_config=job_config).result()
        bq.query(delete_property_query, job_config=job_config).result()

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database delete failed: {str(e)}"
        )

    return {"message": f"Property {property_id} deleted successfully"}