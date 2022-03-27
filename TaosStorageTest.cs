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
    public class TaosStorageTest
    {
        public static int singleTableRecords => 10000_0000;
        public static int totalRecordsWrite = 0;
        public static int totalExecSuccess = 0;
        public static int totalExecError = 0;
        //不同存储模式下的子表数量，即各类数据类型分布比例
        int jobCountInt = 5;
        int jobCountBool = 5;
        int jobCountFloat = 5;
        int jobCountString = 1;
        int jobCount = 0;
        int batchJobCount = 0;
        bool isSingleValuesMode { init; get; }

        class JobData
        {
            public string stable="";
            public string tbname="";
            public int max= 100_0000;
        }
        public TaosStorageTest(bool isSingleValuesMode)
        {
            this.isSingleValuesMode = isSingleValuesMode;

        }
        public void DoTest()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Console.WriteLine($"Taos存储测试开始，{(isSingleValuesMode?"单值模式":"多值模式")}");

            for (int i = 0; i < jobCountInt; i++)
            {
                JobData jobData = new JobData();
                jobData.stable = "ot_int";
                jobData.tbname = $"ti_{i}";
                Thread thread = new Thread(() =>
                {
                    DoTaosThread(jobData);
                });
                thread.Start();
                jobCount++;
            }
            for (int i = 0; i < jobCountBool; i++)
            {
                JobData jobData = new JobData();
                jobData.stable = "ot_bool";
                jobData.tbname = $"tb_{i}";
                jobData.max = 2;
                Thread thread = new Thread(() =>
                {
                    DoTaosThread(jobData);
                });
                thread.Start();
                jobCount++;
            }
            for (int i = 0; i < jobCountFloat; i++)
            {
                JobData jobData = new JobData();
                jobData.stable = "ot_float";
                jobData.tbname = $"tf_{i}";
                Thread thread = new Thread(() =>
                {

                    DoTaosThread(jobData);
                });
                thread.Start();
                jobCount++;
            }
            for (int i = 0; i < jobCountString; i++)
            {
                JobData jobData = new JobData();
                jobData.stable = "ot_string";
                jobData.tbname = $"ts_{i}";
                jobData.max = 100_0000;
                Thread thread = new Thread(() =>
                {
                    DoTaosThread(jobData);
                });
                thread.Start();
                jobCount++;
            }
            while (batchJobCount > 0)
            {
                Thread.Sleep(100);
            }
            sw.Stop();
            Console.WriteLine($"执行完毕, jobCount:{jobCount},totalRecordsWrite: {totalRecordsWrite},totalExecSuccess:{totalExecSuccess},totalExecError:{totalExecError},Elapsed: {sw.Elapsed}");
        }
        private void DoTaosThread(Object obj)
        {
            JobData jobData = (JobData)obj;
            Interlocked.Increment(ref batchJobCount);

            TaosStorageTestJob job = new TaosStorageTestJob();
            job.Do(isSingleValuesMode,jobData.stable, jobData.tbname, jobData.max);

            Interlocked.Decrement(ref batchJobCount);
        }
    }
}
