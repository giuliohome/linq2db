using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Mapping;

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
	
	
	static class ExpressionTestExtensions
	{
		public class LeftJoinInfo<TOuter,TInner>
		{
			public TOuter Outer;
			public TInner Inner;
		}

		[ExpressionMethod("LeftJoinImpl")]
		public static IQueryable<LeftJoinInfo<TOuter,TInner>> LeftJoin<TOuter, TInner, TKey>(
			this IQueryable<TOuter> outer,
			IEnumerable<TInner> inner,
			Expression<Func<TOuter, TKey>> outerKeySelector,
			Expression<Func<TInner, TKey>> innerKeySelector)
		{
			return outer
				.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
				.SelectMany(t => t.gr.DefaultIfEmpty(), (o,i) => new LeftJoinInfo<TOuter,TInner> { Outer = o.o, Inner = i });
		}

		static Expression<Func<
			IQueryable<TOuter>,
			IEnumerable<TInner>,
			Expression<Func<TOuter,TKey>>,
			Expression<Func<TInner,TKey>>,
			IQueryable<LeftJoinInfo<TOuter,TInner>>>>
			LeftJoinImpl<TOuter, TInner, TKey>()
		{
			return (outer,inner,outerKeySelector,innerKeySelector) => outer
				.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
				.SelectMany(t => t.gr.DefaultIfEmpty(), (o,i) => new LeftJoinInfo<TOuter,TInner> { Outer = o.o, Inner = i });
		}

		/*
		[ExpressionMethod("LeftJoinImpl1")]
		public static IQueryable<TResult> LeftJoin<TOuter,TInner,TKey,TResult>(
			this IQueryable<TOuter> outer,
			IEnumerable<TInner> inner,
			Expression<Func<TOuter,TKey>> outerKeySelector,
			Expression<Func<TInner,TKey>> innerKeySelector,
			Expression<Func<TOuter,TInner,TResult>> resultSelector)
		{
			return outer
				.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
				.SelectMany(t => t.gr.DefaultIfEmpty(), (o,i) => o, resultSelector);
		}
		static Expression<Func<
			IQueryable<TOuter>,
			IEnumerable<TInner>,
			Expression<Func<TOuter,TKey>>,
			Expression<Func<TInner,TKey>>,
			Expression<Func<TOuter,TInner, TResult>>,
			IQueryable<TResult>>>
			LeftJoinImpl1<TOuter,TInner,TKey,TResult>()
		{
			return (outer,inner,outerKeySelector,innerKeySelector,resultSelector) => outer
				.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
				.SelectMany(t => t.gr.DefaultIfEmpty(), (o,i) => new { o.o, i })
				.Select(resultSelector);
		}
		*/
	}
	
	
	static class Test {
			
		[ExpressionMethod("MyWhereImpl")]
		static IQueryable<TSource> MyWhere<TSource>(IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
		{
			return source.Where(predicate);
		}

		static Expression<Func<IQueryable<TSource>,Expression<Func<TSource,bool>>,IQueryable<TSource>>> MyWhereImpl<TSource>()
		{
			return (source, predicate) => source.Where(predicate);
		}			
			
			
		static internal IQueryable<Tuple<T1,T2>> Join<T1, T2>(Expression<Func<T1,Func<T2, bool>>> joinCond)
			where T2: class
			where T1 : class
		{
			using (var db = new MyContext()) {
				var query =
					from b in db.GetTable<T1>() //.Where(straight(2))
					from f in db.GetTable<T2>()
					.Where(
						//lambda(b)
						joinCond.Compile()(b)
						//q => q.id == b.id
					)
					select new Tuple<T1,T2> (b,f);
				
				return query;

			}
		}
		
		
		
		static internal IQueryable<ExpressionTestExtensions.LeftJoinInfo<T2,T1>> NewJoin<T1, T2, TKey>(Expression<Func<T2, TKey>> outer, Expression<Func<T1, TKey>> inner)
			where T2: class
			where T1 : class
		{

			
	
			
			using (var db = new MyContext()) {
				
				
			var query = (from b in db.GetTable<T2>() select b).LeftJoin <T2,T1, TKey>((from f in db.GetTable<T1>() select f), outer, inner);

				
				return query;

			}
		}
	}

	
	
	
	
	

	class Program
	{
		public static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");
			
			//var queryList = Test.Join<Bar, Foo>(b => q => q.id == b.id);
						
			
			var queryList =  Test.NewJoin<Bar, Foo, int>(q => q.id, b => b.id);
			
			foreach (var telement in queryList)
			{
				var bar = telement.Inner as Bar;
				var element = telement.Outer as Foo;
				Console.WriteLine(element.id.ToString() + " " + element.FromDate.ToShortDateString() +" "
				                  +bar.id.ToString() + " " + bar.Name
				                 );
			}
			
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
	}
}