/*
 * Created by SharpDevelop.
 * User: MyHome
 * Date: 11/01/2016
 * Time: 18:59
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Linq;
using System.Linq.Expressions;
using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Mapping;
using System.Configuration;
using System.Reflection;

namespace db_test
{
	
	[Table(Name = "Foo")]
	public class Foo {
		
		[PrimaryKey, Column(Name = "id")]
  		public int id { get; set; }
		
  		[Column(Name = "FromDate")]
  		public DateTime FromDate { get; set; }
	}

	public class Bar {
		
		[PrimaryKey, Column(Name = "id")]
  		public int id { get; set; }
		
  		[Column(Name = "name")]
  		public string Name { get; set; }
	}
	
	public class MyContext : LinqToDB.Data.DataConnection
{
  public MyContext() : base("SQLite","Data Source=mydb.s3db") { }

  //public ITable<Foo> foo	 { get { return GetTable<Foo>(); } }

 
  
  
}
	
	static class Test {
        static internal void Join<T1, T2>(Expression<Func<T1,Func<T2, bool>>> joinCond) 
			where T2: class
            where T1 : class
		{


            using (var db = new MyContext()) {
                var query =
                    from b in db.GetTable<T1>() //.Where(straight(2))
                	from f in db.GetTable<T2>().Where(
                		//lambda(b)
                		joinCond.Compile()(b)
                		//q => q.id == b.id
                	) 
					select new Tuple<T1,T2> (b,f);

             var debug_me = query.Expression;
                
				var queryList = query.ToList();
                foreach (Tuple<T1, T2> telement in queryList)
                {
                    var bar = telement.Item1 as Bar;
                    var element = telement.Item2 as Foo;
					Console.WriteLine(element.id.ToString() + " " + element.FromDate.ToShortDateString() +" "
                                    +bar.id.ToString() + " " + bar.Name
                        );
				}
			}			
		}
	}

    

	class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");
			
			
			Test.Join<Bar, Foo>(b => q => q.id == b.id);
			
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
	}
}