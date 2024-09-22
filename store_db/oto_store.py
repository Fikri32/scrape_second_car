import pandas as pd
import psycopg2
from psycopg2 import sql
import pendulum
import os


def get_db_connection():
    """Establish a connection to the PostgreSQL database."""
    conn = psycopg2.connect(
        dbname="scrape_db",
        user="scrape_admin",
        password="admin123",
        host="172.22.37.109",
        port="5432",
    )
    return conn


def create_table_if_not_exists(conn):
    """Create the table in PostgreSQL if it does not exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS car_data (
        Title TEXT,
        Price BIGINT,          -- Changed from INTEGER to BIGINT
        KM BIGINT,             -- Changed from INTEGER to BIGINT
        Fuel_Type TEXT,
        Transmission TEXT,
        Location TEXT,
        Seats INTEGER,
        Engine TEXT,
        Link TEXT,
        Source TEXT
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()


def insert_data_from_dataframe(df, conn):
    """Insert data from a DataFrame into PostgreSQL."""
    insert_query = """
    INSERT INTO car_data (Title, Price, KM, Fuel_Type, Transmission, Location, Seats, Engine, Link, Source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with conn.cursor() as cur:
        for _, row in df.iterrows():
            data_tuple = (
                row["Title"],
                int(row["Price"]) if not pd.isna(row["Price"]) else None,
                int(row["KM"]) if not pd.isna(row["KM"]) else None,
                row["Fuel Type"],
                row["Transmission"],
                row["Location"],
                int(row["Seats"]) if not pd.isna(row["Seats"]) else None,
                row["Engine"],
                row["Link"],
                row["Source"],
            )
            cur.execute(insert_query, data_tuple)
        conn.commit()


def save_to_postgres():
    now = pendulum.now().format("YYYY-MM-DD")
    input_path = f"/home/miracle/mobil/scrape/output/oto/{now}/"
    cleaned_file_name = f"all_second_car_data_{now}.csv"
    cleaned_file_path = os.path.join(input_path, cleaned_file_name)

    # Periksa apakah file ada
    if not os.path.exists(cleaned_file_path):
        print(f"File tidak ada: {cleaned_file_path}")
        return

    # Baca file CSV yang sudah dibersihkan ke dalam DataFrame Pandas
    df = pd.read_csv(cleaned_file_path)

    # Periksa apakah DataFrame kosong
    if df.empty:
        print("DataFrame kosong. Tidak ada data yang dimasukkan.")
        return
    else:
        print(f"DataFrame dimuat dengan {len(df)} data.")

    # Membuat koneksi ke database
    conn = get_db_connection()

    # Siapkan query insert
    insert_query = """
    INSERT INTO scrape_second_car.car_data (Title, Price, KM, Fuel_Type, Transmission, Location, Seats, Engine, Link, Source)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Masukkan data dari DataFrame ke PostgreSQL dengan penanganan kesalahan
    with conn.cursor() as cur:
        for _, row in df.iterrows():
            try:
                cur.execute(insert_query, tuple(row))
                conn.commit()  # Lakukan commit setelah setiap insert yang berhasil
            except Exception as e:
                print(f"Gagal memasukkan baris {row} karena {e}")
                conn.rollback()  # Rollback hanya transaksi yang bermasalah

    print("Data telah disimpan ke PostgreSQL")

    # Tutup koneksi
    conn.close()


if __name__ == "__main__":
    save_to_postgres()
