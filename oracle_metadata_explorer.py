import oracledb
import getpass
import sys

# Database Connection

def connect_to_oracle():
    print("Welcome to Oracle Metadata Explorer!")
    print("------------------------------------")

    host = input("Enter DB host (default: localhost): ").strip() or "localhost"
    port = input("Enter DB port (default: 1521): ").strip() or "1521"
    service = input("Enter service name (e.g. FREEPDB1): ").strip() or "FREEPDB1"
    username = input("Enter username: ").strip()
    password = getpass.getpass("Enter password: ")

    data = f"{host}:{port}/{service}"
    try:
        connection = oracledb.connect(user=username, password=password, dsn=data)
        print("\nConnected successfully!\n")
        return connection
    except oracledb.DatabaseError as e:
        print("Could not connect:", e)
        sys.exit(1)

# Helper Functions

def list_objects(conn, query, column_name):
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    if not rows:
        print("No objects found.")
        return []
    for i, r in enumerate(rows, start=1):
        print(f"{i}. {r[0]}")
    return [r[0] for r in rows]

def choose_from_list(prompt, items):
    if not items:
        return None
    choice = input(f"\n{prompt} (enter number or Enter to cancel): ").strip()
    if not choice:
        return None
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(items):
            return items[idx]
    except ValueError:
        pass
    print("Invalid choice.")
    return None

# Metadata Queries

def show_table_columns(conn, table_name):
    cur = conn.cursor()
    sql = """SELECT COLUMN_ID, COLUMN_NAME, DATA_TYPE, DATA_LENGTH,
             DATA_PRECISION, DATA_SCALE, NULLABLE, DATA_DEFAULT
             FROM USER_TAB_COLUMNS
             WHERE TABLE_NAME = :1
             ORDER BY COLUMN_ID"""
    cur.execute(sql, [table_name.upper()])
    rows = cur.fetchall()
    if not rows:
        print("No columns found.")
        return
    print(f"\nColumns for {table_name}:")
    print(f"{'ID':<3} {'COLUMN_NAME':<25} {'TYPE':<15} {'LEN':<5} {'PREC':<5} {'SCALE':<5} {'NULL':<5} {'DEFAULT':<10}")
    for r in rows:
        print(f"{r[0]:<3} {r[1]:<25} {r[2]:<15} {r[3]:<5} {r[4] or '':<5} {r[5] or '':<5} {r[6]:<5} {str(r[7])[:10] if r[7] else ''}")

def show_table_constraints(conn, table_name):
    cur = conn.cursor()
    sql = """SELECT uc.CONSTRAINT_NAME, uc.CONSTRAINT_TYPE, uc.R_CONSTRAINT_NAME,
             ucc.COLUMN_NAME, uc.SEARCH_CONDITION
             FROM USER_CONSTRAINTS uc
             LEFT JOIN USER_CONS_COLUMNS ucc
             ON uc.CONSTRAINT_NAME = ucc.CONSTRAINT_NAME
             WHERE uc.TABLE_NAME = :1"""
    cur.execute(sql, [table_name.upper()])
    rows = cur.fetchall()
    if not rows:
        print("No constraints found.")
        return
    print(f"\nConstraints for {table_name}:")
    for r in rows:
        print(f"{r[0]} ({r[1]}) column={r[3] or ''} ref={r[2] or ''} cond={r[4] or ''}")

def show_table_indexes(conn, table_name):
    cur = conn.cursor()
    sql = "SELECT INDEX_NAME, UNIQUENESS FROM USER_INDEXES WHERE TABLE_NAME = :1"
    cur.execute(sql, [table_name.upper()])
    indexes = cur.fetchall()
    if not indexes:
        print("No indexes found.")
        return
    for idx, (name, uniq) in enumerate(indexes, start=1):
        print(f"\n{idx}. Index: {name} ({uniq})")
        cur2 = conn.cursor()
        cur2.execute(
            "SELECT COLUMN_NAME, COLUMN_POSITION, DESCEND FROM USER_IND_COLUMNS WHERE INDEX_NAME = :1 ORDER BY COLUMN_POSITION",
            [name]
        )
        for col, pos, desc in cur2:
            print(f"   {pos}. {col} ({desc})")

def show_view_definition(conn, view_name):
    cur = conn.cursor()
    cur.execute("SELECT TEXT FROM USER_VIEWS WHERE VIEW_NAME = :1", [view_name.upper()])
    row = cur.fetchone()
    if row:
        print(f"\nDefinition for {view_name}:\n{'-'*50}\n{row[0]}\n{'-'*50}")
    else:
        print("No definition found.")

def show_sequence_details(conn, seq_name):
    cur = conn.cursor()
    cur.execute("""SELECT SEQUENCE_NAME, MIN_VALUE, MAX_VALUE, INCREMENT_BY, CYCLE_FLAG, ORDER_FLAG, LAST_NUMBER
                   FROM USER_SEQUENCES WHERE SEQUENCE_NAME = :1""", [seq_name.upper()])
    row = cur.fetchone()
    if not row:
        print("Sequence not found.")
        return
    print(f"\nSequence {seq_name}:")
    print(f"Min: {row[1]}, Max: {row[2]}, Increment: {row[3]}, Cycle: {row[4]}, Order: {row[5]}, Last: {row[6]}")
    if input("Fetch NEXTVAL? (y/N): ").lower() == "y":
        cur2 = conn.cursor()
        cur2.execute(f"SELECT {seq_name}.NEXTVAL FROM DUAL")
        val = cur2.fetchone()[0]
        print("NEXTVAL =", val)

def show_user_details(conn, username):
    cur = conn.cursor()
    cur.execute("SELECT USERNAME, USER_ID, CREATED FROM ALL_USERS WHERE USERNAME = :1", [username.upper()])
    row = cur.fetchone()
    if not row:
        print("User not found.")
        return
    print(f"User: {row[0]} (ID={row[1]}) Created: {row[2]}")

# Menus

def handle_tables(conn):
    tables = list_objects(conn, "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME", "TABLE_NAME")
    tbl = choose_from_list("Select a table", tables)
    if not tbl:
        return
    while True:
        print(f"\n-- TABLE: {tbl} --")
        print("1. Columns")
        print("2. Constraints")
        print("3. Indexes")
        print("4. Back")
        opt = input("Choose option: ").strip()
        if opt == "1": show_table_columns(conn, tbl)
        elif opt == "2": show_table_constraints(conn, tbl)
        elif opt == "3": show_table_indexes(conn, tbl)
        elif opt == "4": break
        else: print("Invalid.")

def handle_views(conn):
    views = list_objects(conn, "SELECT VIEW_NAME FROM USER_VIEWS ORDER BY VIEW_NAME", "VIEW_NAME")
    v = choose_from_list("Select a view", views)
    if not v:
        return
    while True:
        print(f"\n-- VIEW: {v} --")
        print("1. Columns")
        print("2. Definition")
        print("3. Back")
        opt = input("Choose option: ").strip()
        if opt == "1": show_table_columns(conn, v)
        elif opt == "2": show_view_definition(conn, v)
        elif opt == "3": break
        else: print("Invalid.")

def handle_sequences(conn):
    seqs = list_objects(conn, "SELECT SEQUENCE_NAME FROM USER_SEQUENCES ORDER BY SEQUENCE_NAME", "SEQUENCE_NAME")
    s = choose_from_list("Select a sequence", seqs)
    if s:
        show_sequence_details(conn, s)

def handle_users(conn):
    users = list_objects(conn, "SELECT USERNAME FROM ALL_USERS ORDER BY USERNAME", "USERNAME")
    u = choose_from_list("Select a user", users)
    if u:
        show_user_details(conn, u)



def main():
    conn = connect_to_oracle()
    while True:
        print("\nMAIN MENU")
        print("1. Tables")
        print("2. Views")
        print("3. Sequences")
        print("4. Users")
        print("5. Exit")
        opt = input("Enter option: ").strip()
        if opt == "1": handle_tables(conn)
        elif opt == "2": handle_views(conn)
        elif opt == "3": handle_sequences(conn)
        elif opt == "4": handle_users(conn)
        elif opt == "5":
            print("Goodbye!")
            conn.close()
            break
        else:
            print("Invalid option.")

if __name__ == "__main__":
    main()