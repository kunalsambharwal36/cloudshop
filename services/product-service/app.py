from fastapi import FastAPI, HTTPException
from sqlalchemy import create_engine, text
import os
from prometheus_fastapi_instrumentator import Instrumentator
app = FastAPI()
Instrumentator().instrument(app).expose(app)

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:admin123@localhost:5432/cloudshop"
)

engine = create_engine(DATABASE_URL)

@app.get("/")
def root():
    return {"message": "Product Service Running"}


@app.get("/products")
def products():

    with engine.connect() as conn:
        result = conn.execute(
            text("select * from products")
        )

        return [
            dict(row._mapping)
            for row in result
        ]


@app.get("/products/{product_id}")
def get_product(product_id: int):

    with engine.connect() as conn:

        result = conn.execute(
            text("select * from products where id=:id"),
            {"id": product_id}
        )

        row = result.fetchone()

        if not row:
            raise HTTPException(
                status_code=404,
                detail="Product not found"
            )

        return dict(row._mapping)


@app.post("/products")
def create_product(product_name: str,
                   price: float,
                   stock: int):

    with engine.begin() as conn:

        conn.execute(
            text("""
            insert into products
            (product_name,price,stock)
            values
            (:name,:price,:stock)
            """),
            {
                "name": product_name,
                "price": price,
                "stock": stock
            }
        )

    return {
        "message": "Product created"
    }


@app.delete("/products/{product_id}")
def delete_product(product_id: int):

    with engine.begin() as conn:

        conn.execute(
            text(
                "delete from products where id=:id"
            ),
            {"id": product_id}
        )

    return {
        "message": "Product deleted"
    }
