using System;

namespace PascalmingTaosStorageTest
{
    class Program
    {
        static void Main(string[] args)
        {
            bool isMulti = false;
            bool isSingle = false;

            foreach (string arg in args)
            {
                if (arg.Equals("-single"))
                    isSingle = true;
                if (arg.Equals("-multi"))
                    isMulti = true;
            }
            if (isSingle == false && isMulti == false)
            {
                Console.WriteLine("参数不正确,use:");
                Console.WriteLine("dotnet PascalMingTaosStorageTest.dll [-single|-multi]");
                return;
            }

            TaosStorageTest taosStorageTest = new TaosStorageTest(isSingle);

            taosStorageTest.DoTest();
        }
    }


}