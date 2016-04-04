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
        static internal void This<T1, T2>(Func<T1, Expression<Func<T2, bool>>> lambda, Func<int,Expression<Func<T1, bool>>> straight) // 
			where T2: Foo//class
            where T1 : Bar//class
		{
            var pf = Expression.Parameter(typeof(T2),"f");
            var pb = Expression.Parameter(typeof(T1), "b");
            PropertyInfo FooId = typeof(T2).GetProperty("id");
            PropertyInfo BarId = typeof(T1).GetProperty("id");
            var eqexpr = Expression.Equal(Expression.Property(pf, FooId), Expression.Property(pb, BarId));
            var lambdaInt = Expression.Lambda<Func<T2, bool>>(eqexpr, pf);
            var lambdaExpr = Expression.Lambda<Func<T1,Func<T2, bool>>>( lambdaInt,pb);
			

            using (var db = new MyContext()) {
                var query =
                    from b in db.GetTable<T1>().Where(straight(2))
                	from f in db.GetTable<T2>().Where(
                		//lambda(b)
                		lambdaExpr.Compile()(b)
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
			
			// TODO: Implement Functionality Here
			
			
			Test.This<Bar, Foo>( k => (q => q.id == k.id), q => (a => a.id == q));

			
			
			
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
	}
}