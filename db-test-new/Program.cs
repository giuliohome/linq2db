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
		
	}
	
	static class Test {
		static internal IQueryable<Tuple<T1,T2>> Join<T1, T2>(Expression<Func<T1,Func<T2, bool>>> joinCond)
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
				
				return query;

			}
		}
	}

	

	class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");
			
			var queryList = Test.Join<Bar, Foo>(b => q => q.id == b.id);
			
			
			foreach (Tuple<Bar, Foo> telement in queryList)
			{
				var bar = telement.Item1 as Bar;
				var element = telement.Item2 as Foo;
				Console.WriteLine(element.id.ToString() + " " + element.FromDate.ToShortDateString() +" "
				                  +bar.id.ToString() + " " + bar.Name
				                 );
			}
			
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
	}
}