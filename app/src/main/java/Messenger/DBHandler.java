package Messenger;


import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

/**
 * Created by prati on 3/4/2018.
 */

public class DBHandler extends SQLiteOpenHelper{

    public DBHandler(Context context, String name, SQLiteDatabase.CursorFactory factory, int version) {

        super(context,"MessengerDB", factory, version);

        // Log.v("db", "Constructor call");
    }

    @Override
    public void onCreate(SQLiteDatabase db) {

        // Log.v("db", "On create start");
        db.execSQL("DROP TABLE IF EXISTS Tb_KeyPair");
        String CreateTable = "Create table Tb_KeyPair(" +
                "key TEXT UNIQUE,"+
                "value TEXT)";

        db.execSQL(CreateTable);
        // Log.v("db", "Table created");
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion,
                          int newVersion) {

        db.execSQL("DROP TABLE IF EXISTS Tb_KeyPair");
        onCreate(db);
    }
}