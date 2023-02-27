package semipg

import (
	"database/sql"
	"log"
	"testing"
	//_ "github.com/frozen/semipg"

	"github.com/stretchr/testify/require"
)

func TestInsertDifferentOrder(t *testing.T) {
	//connStr := "semipg://<username>:<password>@<database_ip>/todos?sslmode=disable"
	connStr := "//<username>:<password>@<database_ip>/todos?sslmode=disable"
	// Connect to database
	db, err := sql.Open("semipg", connStr)
	if err != nil {
		log.Fatal(err)
	}
	//db := NewDB()

	t.Log(db.Exec("create table item(id int, name text);"))
	t.Log(db.Exec("insert into item(id, name) values(1, 'item1');"))
	t.Log(db.Exec("insert into item(name, id) values('item1', 1);"))

	rows, err := db.Query("select id, name from item")
	if err != nil {
		log.Fatal(err)
	}

	if ok := rows.Next(); ok {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		require.Equal(t, 1, id)
		require.Equal(t, name, "item1")
	} else {
		log.Println("no row")
	}

	if ok := rows.Next(); ok {
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		require.Equal(t, 1, id)
		require.Equal(t, name, "item1")
	} else {
		t.Error("no row")
	}

	if ok := rows.Next(); ok {
		t.Error("should not have more rows")
	}
}

func TestPreparedStmt(t *testing.T) {
	//connStr := "semipg://<username>:<password>@<database_ip>/todos?sslmode=disable"
	connStr := "//<username>:<password>@<database_ip>/todos?sslmode=disable"
	// Connect to database
	db, err := sql.Open("semipg", connStr)
	if err != nil {
		log.Fatal(err)
	}

	//db := NewDB()
	db.Exec("create table item(id int, name text);")
	db.Exec("insert into item(id, name) values(1, 'item1');")
	db.Exec("insert into item(name, id) values('item1', 1);")

	stmt, err := db.Prepare("select * from item where id = $1")
	require.NoError(t, err)

	rows, err := stmt.Query(1)
	require.NoError(t, err)

	i := 2
	for rows.Next() {
		i++
		var id int
		var name string
		err = rows.Scan(&id, &name)
		if err != nil {
			log.Fatal(err)
		}
		require.Equal(t, 1, id)
		require.Equal(t, name, "item1")
	}

	require.Equal(t, 2, i)
}
