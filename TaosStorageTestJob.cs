using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TDengineDriver;

namespace PascalmingTaosStorageTest
{
    public class TaosStorageTestJob
    {
        //private string host = "127.0.0.1";
        private string host = "192.168.58.128";
        private string user = "root";
        private string password = "taosdata";
        private short port = 0;
        private string dbName = "testdb";
        private IntPtr conn = IntPtr.Zero;
        public int Records { get; private set; }

        //最大SQL Value数量
        private readonly int maxBatchSqlValues = 1000;

        public void Do(bool isSingleValuesMode,string stable, string tbname, int max)
        {
            InitTDengine();
            ConnectTDengine();
            useDatabase();
            if (isSingleValuesMode)
                DoSingleValues(stable, tbname, max);
            else
                DoMultiValuede(stable, tbname, max);
            CloseConnection();
        }
        /// <summary>
        /// 单值模式
        /// 超级表定义
        /// CREATE TABLE IF NOT EXISTS ot_int(ts timestamp, wts timestamp, v int, counter int, vquality int) TAGS(item binary(128));
        /// CREATE TABLE IF NOT EXISTS ot_float(ts timestamp, wts timestamp, v float, counter int, vquality int) TAGS(item binary(128));
        /// CREATE TABLE IF NOT EXISTS ot_string(ts timestamp, wts timestamp, v binary(128), counter int,vquality int) TAGS(item binary(128));
        /// CREATE TABLE IF NOT EXISTS ot_bool(ts timestamp, wts timestamp, v bool, counter int, vquality int) TAGS(item binary(128));

        /// </summary>
        /// <param name="stable"></param>
        /// <param name="tbname"></param>
        /// <param name="max"></param>
        public void DoSingleValues(string stable, string tbname, int max)
        {
            //Ticks 单位是 100 毫微秒。表示自 0001 年 1 月 1 日午夜 12:00:00 以来已经过的时间的以 100 毫微秒为间隔的间隔数
            long ticks = (DateTime.Now.AddDays(-10).Ticks - new DateTime(1970, 1, 1, 0, 0, 0, 0).Ticks) / 10_000;


            string sqlHead = $"INSERT INTO {tbname} USING {stable} TAGS (\"{tbname}\") VALUES ";
            StringBuilder sqlValue = new StringBuilder();

            Random rand = new Random();
            bool isString = stable.EndsWith("ot_string");
            bool isBool = stable.EndsWith("ot_bool");

            for (int i = 0; i < TaosStorageTest.singleTableRecords; i++)
            {
                if (isString)
                {
                    sqlValue.Append($"({ticks},{ticks + 1},'{rand.Next(max)}',{i},192) ");
                }
                else if (isBool)
                {
                    sqlValue.Append($"({ticks},{ticks + 1},{(rand.Next(max) == 0)},{i},192) ");
                }
                else
                {
                    sqlValue.Append($"({ticks},{ticks + 1},{rand.Next(max)},{i},192) ");
                }
                ticks++;

                int count = Interlocked.Increment(ref TaosStorageTest.totalRecordsWrite);
                //批写入
                if ((i % maxBatchSqlValues) == (maxBatchSqlValues - 1))
                {
                    execute(sqlHead + sqlValue);
                    sqlValue.Clear();
                }
                if ((count + 1) % 1000_0000 == 0)
                {
                    System.Console.WriteLine($"total write records [kw]: {(count+1) / 1000_0000}");
                }
            }
            if (sqlValue.Length > 0)
            {
                execute(sqlHead + sqlValue);
                sqlValue.Clear();
            }

        }
        /// <summary>
        /// 多值模式
        /// 超级表定义：CREATE TABLE IF NOT EXISTS ot(ts timestamp, wts timestamp, vint int, vfloat float,vbool bool,vstring binary(128),counter int,vquality int) 
        ///                                        TAGS (item binary(128),vtype  binary(16));
        /// </summary>
        /// <param name="stable"></param>
        /// <param name="tbname"></param>
        /// <param name="max"></param>
        public void DoMultiValuede(string stable, string tbname, int max)
        {
            long ticks = (DateTime.Now.AddDays(-10).Ticks - new DateTime(1970, 1, 1, 0, 0, 0, 0).Ticks) / 10_000;

            
            string sqlHead = $"INSERT INTO {tbname} USING ot TAGS (\"{tbname}\",\"{stable}\") VALUES ";
            StringBuilder sqlValue = new StringBuilder();

            Random rand = new Random();
            bool isString = stable.EndsWith("ot_string");
            bool isBool = stable.EndsWith("ot_bool");
            bool isFloat = stable.EndsWith("ot_float");
            for (int i = 0; i < TaosStorageTest.singleTableRecords; i++)
            {
                //ts,wts,vint,vfloat,vbool,vstring,counter,vquality
                if (isString)
                {
                    sqlValue.Append($"({ticks},{ticks + 1},null,null,null,'{rand.Next(max)}',{i},192) ");
                }
                else if (isBool)
                {
                    sqlValue.Append($"({ticks},{ticks + 1},null,null,{(rand.Next(2) == 0)},null,{i},192) ");
                }
                else if (isFloat)
                {
                     sqlValue.Append($"({ticks},{ticks + 1},null,{rand.Next(max)},null,null,{i},192) ");
               }
                else
                {
                    sqlValue.Append($"({ticks},{ticks + 1},{rand.Next(max)},null,null,null,{i},192) ");
                }
                ticks++;

                int count = Interlocked.Increment(ref TaosStorageTest.totalRecordsWrite);
                //批写入
                if ((i % maxBatchSqlValues) == (maxBatchSqlValues - 1))
                {
                    execute(sqlHead + sqlValue);
                    sqlValue.Clear();
                }
                if ((count + 1) % 1000_0000 == 0)
                {
                    System.Console.WriteLine($"总写入记录数 [千万]: {(count + 1) / 1000_0000}");
                }
            }
            if (sqlValue.Length > 0)
            {
                execute(sqlHead + sqlValue);
                sqlValue.Clear();
            }

        }

        public void InitTDengine()
        {
            TDengine.Init();
        }

        public void ConnectTDengine()
        {
            string db = "";
            this.conn = TDengine.Connect(this.host, this.user, this.password, db, this.port);
            if (this.conn == IntPtr.Zero)
            {
                Console.WriteLine("连接失败: " + this.host);
                System.Environment.Exit(0);
            }
            else
            {
                //Console.WriteLine("[ OK ] Connection established.");
            }
        }
        public void useDatabase()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("use ").Append(this.dbName);
            execute(sql.ToString(), false);
        }
        public void execute(string sql, bool isInsert = true)
        {
            IntPtr res = TDengine.Query(this.conn, sql);

            if ((res == IntPtr.Zero) || (TDengine.ErrorNo(res) != 0))
            {
                if (isInsert == true)
                {
                    Interlocked.Increment(ref TaosStorageTest.totalExecError);
                }
                Console.Write(sql + " failure, ");
                if (res != IntPtr.Zero)
                {
                    Console.Write("reason: " + TDengine.Error(res));
                }
                Console.WriteLine("");
            }
            else
            {
                if (isInsert == true)
                {
                    Interlocked.Increment(ref TaosStorageTest.totalExecSuccess);
                }
            }
            TDengine.FreeResult(res);
        }
        public void CloseConnection()
        {
            if (this.conn != IntPtr.Zero)
            {
                TDengine.Close(this.conn);
            }
        }
    }
}
