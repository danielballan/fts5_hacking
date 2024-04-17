from sqlalchemy import (
    Column,
    JSON,
    Integer,
    Table,
    event,
    create_engine,
    select,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Session, declarative_base

# Use JSON with SQLite and JSONB with PostgreSQL.
JSONVariant = JSON().with_variant(JSONB(), "postgresql")

Base = declarative_base()


class Node(Base):
    """
    This describes a single Node and sometimes inlines descriptions of all its children.
    """

    __tablename__ = "nodes"
    __mapper_args__ = {"eager_defaults": True}

    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    metadata_ = Column("metadata", JSONVariant, nullable=False)


@event.listens_for(Node.metadata, "after_create")
def create_virtual_table_fits5(target, connection, **kw):
    print("create_virtual_table_fits5")
    if connection.engine.dialect.name == "sqlite":
        statements = [
            # Create an external content fts5 table.
            # See https://www.sqlite.org/fts5.html Section 4.4.3.
            """
            CREATE VIRTUAL TABLE metadata_fts5 USING fts5(metadata, content='nodes', content_rowid='id');
            """,
            # Triggers keep the index synchronized with the nodes table.
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_ai AFTER INSERT ON nodes BEGIN
              INSERT INTO metadata_fts5(rowid, metadata)
              VALUES (new.id, new.metadata);
            END;
            """,
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_ad AFTER DELETE ON nodes BEGIN
              INSERT INTO metadata_fts5(metadata_fts5, rowid, metadata)
              VALUES('delete', old.id, old.metadata);
            END;
            """,
            """
            CREATE TRIGGER nodes_metadata_fts5_sync_au AFTER UPDATE ON nodes BEGIN
              INSERT INTO metadata_ft5_index(metadata_ft5_index, rowid, metadata)
              VALUES('delete', old.id, old.metadata);
              INSERT INTO metadata_ft5_index(rowid, metadata)
              VALUES (new.id, new.metadata);
            END;
            """,
        ]
        for statement in statements:
            connection.execute(text(statement))


def main():
    engine = create_engine("sqlite:///./test.db", echo=True)
    with engine.connect() as connection:
        Base.metadata.create_all(connection)
        connection.commit()

    # Connect to the existing metadata_fts5 virtual table.
    # This must be run *after* Base.create_all is called above.
    metadata_fts5 = Table(
        "metadata_fts5",
        Node.metadata,
        Column("rowid", Integer, primary_key=True),
        Column("metadata", JSONVariant),
    )
    # Insert rows.
    with Session(engine) as session:
        for color in ["red", "green", "blue"]:
            session.add(Node(metadata_={"color": color}))
            session.commit()
    # Search.
    with Session(engine) as session:
        statement = select(metadata_fts5.c.metadata).where(
            metadata_fts5.c.metadata.match("red")
        )
        result = session.execute(statement).all()
        print(result)


if __name__ == "__main__":
    main()
