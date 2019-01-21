package main

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"golang.org/x/net/proxy"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

type Config struct {
	DBsocks string
	DBuser  string
	DBpass  string
	DBhost  string
	DBname  string
}

//InitDB starts the db connection
func InitDB(c *Config) (*sqlx.DB, error) {
	//fmt.Println(c.DBsocks)
	fixieData := strings.Split(c.DBsocks, "@")
	fixieAddr := fixieData[1]
	authData := strings.Split(fixieData[0], ":")
	auth := proxy.Auth{
		User:     authData[0],
		Password: authData[1],
	}

	dialer, err := proxy.SOCKS5("tcp", fixieAddr, &auth, proxy.Direct)
	if err != nil {
		fmt.Fprintln(os.Stderr, "can't connect to the proxy:", err)
		os.Exit(1)
	}

	mysql.RegisterDial("fixieDial", func(addr string) (net.Conn, error) {
		return dialer.Dial("tcp", addr)
	})

	cnx := fmt.Sprintf("%s:%s@fixieDial(%s)/%s?allowOldPasswords=true",
		c.DBuser, c.DBpass, c.DBhost, c.DBname)

	//fmt.Println(cnx)

	db, err := sqlx.Connect("mysql", cnx)
	if err != nil {
		fmt.Println("Could not connect")
		log.Fatalln(err)
		return nil, err
	}

	fmt.Println("Connection to db succeed")

	return db, nil
}

type QueryLogger struct {
	queryer sqlx.Queryer
	logger  *log.Logger
}

func (p *QueryLogger) Query(query string, args ...interface{}) (*sql.Rows, error) {
	p.logger.Print(query, args)
	return p.queryer.Query(query, args...)
}

func (p *QueryLogger) Queryx(query string, args ...interface{}) (*sqlx.Rows, error) {
	p.logger.Print(query, args)
	return p.queryer.Queryx(query, args...)
}

func (p *QueryLogger) QueryRowx(query string, args ...interface{}) *sqlx.Row {
	p.logger.Print(query, args)
	return p.queryer.QueryRowx(query, args...)
}

type ExeLogger struct {
	execer sqlx.Execer
	logger *log.Logger
}

func (e *ExeLogger) Exec(query string, args ...interface{}) (sql.Result, error) {
	e.logger.Print(query, args)
	return e.execer.Exec(query, args...)
}

type Process struct {
	ID           int64   `json:"id" db:"ID"`
	User         string  `json:"user" db:"USER"`
	Host         string  `json:"host" db:"HOST"`
	DB           *string `json:"db" db:"DB"`
	Command      string  `json:"command" db:"COMMAND"`
	Time         int64   `json:"time" db:"TIME"`
	State        string  `json:"state" db:"STATE"`
	Info         *string `json:"info" db:"INFO"`
	TimeMs       string  `json:"time_ms" db:"TIME_MS"`
	RowsSent     string  `json:"rows_sent" db:"ROWS_SENT"`
	RowsExamined string  `json:"rows_examined" db:"ROWS_EXAMINED"`
}

func main() {
	c := &Config{
		DBsocks: ":@127.0.0.1:1080",
		DBhost:  "127.0.0.1:3306",
		DBname:  "mysql",
		DBpass:  "111111",
		DBuser:  "root",
	}
	db, err := InitDB(c)
	_ = err

	el := &ExeLogger{db, log.New(os.Stdout, "", 3)}

	schema := `create table if not exists yuntu.delivery(
               id int NOT NULL AUTO_INCREMENT,
               sn varchar(64),
               flow_id int(8),
               buy_date timestamp,
               delivery_time timestamp,
               PRIMARY KEY (id),
               UNIQUE (sn)
            )ENGINE=InnoDB`

	res := sqlx.MustExec(el, schema)

	log.Println(res)

	ql := &QueryLogger{db, log.New(os.Stdout, "", 3)}

	ps := []Process{}

	err = sqlx.Select(ql, &ps, "select * from information_schema.processlist")
	if err != nil {
		log.Fatalln(err)
	} else {
		for _, p := range (ps) {
			log.Printf("ID:%d %+v", p.ID, p)
			if p.DB != nil {
				log.Printf("ID:%d %+v", p.ID, *p.DB)
			}
			if p.Info != nil {
				log.Printf("ID:%d %+v", p.ID, *p.Info)
			}
		}
	}
	/* rows, err := db.Queryx("select * from information_schema.processlist")
	   // Get column names
	   columns, err := rows.Columns()
	   if err != nil {
		  panic(err.Error()) // proper error handling instead of panic in your app
	   }
	   // Make a slice for the values
	   values := make([]interface{}, len(columns))
	   // rows.Scan wants '[]interface{}' as an argument, so we must copy the
	   // references into such a slice
	   // See http://code.google.com/p/go-wiki/wiki/InterfaceSlice for details
	   scanArgs := make([]interface{}, len(values))
	   for i := range values {
		  scanArgs[i] = &values[i]
	   }
	   table := tablewriter.NewWriter(os.Stdout)
	   for rows.Next() {
		  err = rows.Scan(scanArgs...)
		  if err != nil {
			 fmt.Println(err)
			 continue
		  }
		  line := []string{}
		  for i := 0; i < len(values); i++ {
			 line = append(line, printValue(scanArgs[i].(*interface{})))
		  }
		  //打印内容
		  table.Append(line)
	   }
	   table.Render()*/

}

func printValue(pval *interface{}) string {
	var s_txt string
	switch v := (*pval).(type) {
	case nil:
		s_txt = "NULL"
	case time.Time:
		s_txt = "'" + v.Format("2006-01-02 15:04:05.999") + "'"
	case int, int8, int16, int32, int64, float32, float64, byte:
		s_txt = fmt.Sprint(v)
	case []byte:
		s_txt = string(v)
	case bool:
		if v {
			s_txt = "'1'"
		} else {
			s_txt = "'0'"
		}
	default:
		s_txt = "'" + fmt.Sprint(v) + "'"
	}
	return s_txt
}
