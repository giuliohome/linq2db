﻿using System;
using System.Collections.Generic;
using System.Configuration;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using LinqToDB;
using LinqToDB.Common;
using LinqToDB.Mapping;

namespace db_test
{
	
	[Table(Name = "Foo")]
	public class Foo //: IEqualityComparer<Foo>
	{
		
		[PrimaryKey, Column(Name = "id"), NotNull]
		public int id { get; set; }
		
		[Column(Name = "FromDate"), NotNull]
		public DateTime FromDate { get; set; }
		
		[Column(Name = "col_foo"), NotNull]
		public string ColFoo { get; set; }
		
		#region comparer
		//		[NotColumn]
//		public MyKey FooKey {
//			get {
//				return new MyKey(this.id, this.ColFoo);
//			}
//		}
//
//		public bool Equals(Foo x, Foo y)
//		{
//			Debug.WriteLine("Foo eq {0}",x.id == y.id && x.ColFoo.Equals(y.ColFoo) && DateTime.Compare(x.FromDate, y.FromDate) == 0);
//			return x.id == y.id && x.ColFoo.Equals(y.ColFoo) && DateTime.Compare(x.FromDate, y.FromDate) == 0;
//		}
//
//		public int GetHashCode(Foo obj)
//		{
//			return obj.id.GetHashCode() ^ obj.ColFoo.GetHashCode() ^ obj.FromDate.GetHashCode();
//		}
		#endregion
		
	}

	
	public class MyKey //: IEqualityComparer<MyKey>
	{
		public int id;
		public string col;
		
		#region comparer
		//		public MyKey(int _id, string _col) {
//			id = _id;
//			col = _col;
//		}
		
//		public bool Equals(MyKey x, MyKey y)
//		{
//			return x.id == y.id && x.col.Equals(y.col) ;
//		}
//
//		public int GetHashCode(MyKey obj)
//		{
//			return obj.id.GetHashCode() ^ obj.col.GetHashCode();
//		}
		#endregion

	}
	
	public class Bar //: IEqualityComparer<Bar>
	{
		
		[PrimaryKey, Column(Name = "id"), NotNull]
		public int id { get; set; }
		
		[Column(Name = "name"), NotNull]
		public string Name { get; set; }
		
		[Column(Name = "col_bar"), NotNull]
		public string ColBar { get; set; }
		
		#region comparer
//		[NotColumn]
//		public MyKey BarKey {
//			get {
//				return new MyKey(this.id, this.ColBar);
//			}
//		}
//
//		public bool Equals(Bar x, Bar y)
//		{
//			Debug.WriteLine("Bar eq {0}",x.id == y.id && x.ColBar.Equals(y.ColBar) && x.Name.Equals(y.Name));
//			return x.id == y.id && x.ColBar.Equals(y.ColBar) && x.Name.Equals(y.Name);
//		}
//
//		public int GetHashCode(Bar obj)
//		{
//			return obj.id.GetHashCode() ^ obj.ColBar.GetHashCode() ^ obj.Name.GetHashCode();
//		}
		#endregion
	}
	

	
	public class MyContext : LinqToDB.Data.DataConnection
	{
		public MyContext() : base("SQLite","Data Source=mydb.s3db") { }
		
	}
	
	
	static class ExpressionTestExtensions
	{
		
		#region with LeftJoinInfo
		/*
		[ExpressionMethod("LeftJoinImpl")]
 		public class LeftJoinInfo<TOuter,TInner>
		{
			public TOuter Outer;
			public TInner Inner;
		}

		public static IQueryable<LeftJoinInfo<TOuter,TInner>> LeftJoin<TOuter, TInner, TKey>(
			this IQueryable<TOuter> outer,
			IEnumerable<TInner> inner,
			Expression<Func<TOuter, TKey>> outerKeySelector,
			Expression<Func<TInner, TKey>> innerKeySelector)
		{
			return outer
				.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
				.SelectMany(t => t.gr//.DefaultIfEmpty()
				            , (o,i) => new LeftJoinInfo<TOuter,TInner> { Outer = o.o, Inner = i });
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
				.SelectMany(t => t.gr//.DefaultIfEmpty()
				            , (o,i) => new LeftJoinInfo<TOuter,TInner> { Outer = o.o, Inner = i });
		}*/
		#endregion

		[ExpressionMethod("InnerJoinImpl")]
		public static IEnumerable<TResult> InnerJoin<TOuter,TInner,TKey,TResult>(
			this IQueryable<TOuter> outer,
			IEnumerable<TInner> inner,
			Expression<Func<TOuter,TKey>> outerKeySelector,
			Expression<Func<TInner,TKey>> innerKeySelector,
			Expression<Func<TOuter,TInner,TResult>> resultSelector)
		{
			return outer
				.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
				.SelectMany(t => t.gr//.DefaultIfEmpty()
				            , (o,i) => resultSelector.Compile()(o.o,i));
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
				.SelectMany(t => t.gr//.DefaultIfEmpty()
				            , (o,i) => resultSelector.Compile()(o.o,i));
			//.Select<TResult>((o,i) => resultSelector.Compile()(o,i));
		}
		
	}
	
	
	static class Test {
		
		
		#region my where
//		[ExpressionMethod("MyWhereImpl")]
//		static IQueryable<TSource> MyWhere<TSource>(IQueryable<TSource> source, Expression<Func<TSource, bool>> predicate)
//		{
//			return source.Where(predicate);
//		}
//
//		static Expression<Func<IQueryable<TSource>,Expression<Func<TSource,bool>>,IQueryable<TSource>>> MyWhereImpl<TSource>()
//		{
//			return (source, predicate) => source.Where(predicate);
//		}
//
//
//		static internal IQueryable<Tuple<T1,T2>> Join<T1, T2>(Expression<Func<T1,Func<T2, bool>>> joinCond)
//			where T2: class
//			where T1 : class
//		{
//			IQueryable<Tuple<T1,T2>> query = null;
//			using (var db = new MyContext()) {
//
//
//					query =
//					from b in db.GetTable<T1>() //.Where(straight(2))
//					from f in db.GetTable<T2>()
//					.Where(
//						//lambda(b)
//						joinCond.Compile()(b)
//						//q => q.id == b.id
//					)
//					select new Tuple<T1,T2> (b,f);
//
//
//
//			}
//			return query;
//		}
		#endregion

		
		static internal IEnumerable<TResult> 
			NewJoin<T1, T2, TKey, TResult>
			(Expression<Func<T2, TKey>> outer, Expression<Func<T1, TKey>> inner, Expression<Func<T2,T1,TResult>> resultSelector)
			where T2: class
			where T1 : class
		{

			using (var db = new MyContext()) {

				var query = (from b in db.GetTable<T2>() select b).InnerJoin <T2,T1, TKey, TResult>((from f in db.GetTable<T1>() select f), outer, inner, resultSelector);//.AsEnumerable();
	
				#region only for debugging purposes - to be deleted
				//Console.WriteLine(String.Format("Last Query: {0}",db.LastQuery));
				#endregion
				
				return query;

			}
		}
		
		
	}

	
	#region comparer
//		class Compare : IEqualityComparer<Tuple<Bar,Foo>>
	//{
	//    public bool Equals(Tuple<Bar,Foo> x, Tuple<Bar,Foo> y)
	//    {
	//    	Debug.WriteLine("Tuple eq {0}",x.Item1.Equals(x.Item1,y.Item1) && x.Item2.Equals(x.Item2,y.Item2));
	//    	return x.Item1.Equals(x.Item1,y.Item1) && x.Item2.Equals(x.Item2,y.Item2);
	//    }
	//    public int GetHashCode(Tuple<Bar,Foo> codeh)
	//    {
	//    	return codeh.Item1.GetHashCode(codeh.Item1) ^ codeh.Item2.GetHashCode(codeh.Item2);
	//    }
	//}
//
//
//	class NewCompare : IEqualityComparer<ExpressionTestExtensions.LeftJoinInfo<Foo,Bar>>
	//{
	//    public bool Equals(ExpressionTestExtensions.LeftJoinInfo<Foo,Bar> x, ExpressionTestExtensions.LeftJoinInfo<Foo,Bar> y)
	//    {
	//    	Debug.WriteLine("Tuple eq {0}",x.Inner.Equals(x.Inner,y.Inner) && x.Outer.Equals(x.Outer,y.Outer));
	//    	return x.Inner.Equals(x.Inner,y.Inner) && x.Outer.Equals(x.Outer,y.Outer);
	//    }
	//    public int GetHashCode(ExpressionTestExtensions.LeftJoinInfo<Foo,Bar> codeh)
	//    {
	//    	return codeh.Inner.GetHashCode(codeh.Inner) ^ codeh.Outer.GetHashCode(codeh.Outer);
	//    }
	//}
	#endregion




	class Program
	{

		
		private static void ConsoleOut(IEnumerable<JoinClass<Bar,Foo>> queryList) {
			if (queryList != null && queryList.Count()>0) {
				foreach (var telement in queryList)
				{
					var bar = telement.t1;
					var foo = telement.t2;
					Console.WriteLine(foo.id.ToString() + " " + foo.FromDate.ToShortDateString() +" "+ " " + foo.ColFoo + " <==> "
					                  +bar.id.ToString() + " " + bar.Name + " " + bar.ColBar
					                 );
				}
			}
		}
		public class JoinClass<T1,T2> {
			public T1 t1;
			public T2 t2;
		}
		public class JoinCond {
			public string T1Property;
			public string T2Property;
		}
		public static IQueryable<JoinClass<T1,T2>> MultiJoin<T1,T2> (
			JoinCond[] joinConds
		)
			where T2: class
			where T1 : class			
		{
			var jfb = Expression.Parameter(typeof(JoinClass<T1,T2>),"jfb");
			
			FieldInfo ItemT1 = typeof(JoinClass<T1,T2>).GetField("t1");
			FieldInfo ItemT2 = typeof(JoinClass<T1,T2>).GetField("t2");
			
			var pb = Expression.Field(jfb, ItemT1);//
			var pf = Expression.Field(jfb, ItemT2);//
			
			PropertyInfo[] T1Props = new PropertyInfo[joinConds.Length];
			PropertyInfo[] T2Props = new PropertyInfo[joinConds.Length];
			BinaryExpression[] OnJoinEqs = new BinaryExpression[joinConds.Length];
			
			
			for (int i = 0; i < joinConds.Length; i++) {
				T1Props[i] = typeof(T1).GetProperty(joinConds[i].T1Property);
				T2Props[i] = typeof(T2).GetProperty(joinConds[i].T2Property);
				OnJoinEqs[i] = Expression.Equal(Expression.Property(pb, T1Props[i] ), Expression.Property(pf, T2Props[i]));
			}
			
			Expression JoinedAND = OnJoinEqs[0];
			if (joinConds.Length>1) {
				for (int i = 1; i < joinConds.Length; i++) {
					JoinedAND = Expression.AndAlso(JoinedAND, OnJoinEqs[i]);
				}
			}
			
			
			var WhereExpr = Expression.Lambda<Func<JoinClass<T1,T2>,bool>>(JoinedAND, jfb);
			
			using (var db = new MyContext()) {
				
				var q = from b in db.GetTable<T1>()
					from f in db.GetTable<T2>() 
					select new JoinClass<T1,T2>() { t1 = b ,t2=f} ;
				var w = q.Where(WhereExpr);
				
				#region only for debugging purposes - to be deleted
				//w.ToArray();
				//Console.WriteLine(String.Format("Last Query: {0}",db.LastQuery));
				#endregion
				
				return w;
			}
		}
		
		
		public static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");
			JoinCond[] conds1 = new JoinCond[1];
			
			
			Console.WriteLine("by Col string");
			conds1[0] = new JoinCond() { T1Property = "ColBar", T2Property="ColFoo" };
			var queryJoinByCol = MultiJoin<Bar,Foo>(conds1);  
				//Test.NewJoin<Bar, Foo, string, Tuple<Bar,Foo>>(q => q.ColFoo, b => b.ColBar, (f, b) => new Tuple<Bar,Foo>(b,f));
			ConsoleOut(queryJoinByCol);
			Console.WriteLine("-------------");
			
			Console.WriteLine("by Id int");
			conds1[0] = new JoinCond() { T1Property = "id", T2Property="id" };
			var queryJoinById =  MultiJoin<Bar,Foo>(conds1); 
				//Test.NewJoin<Bar, Foo, int, Tuple<Bar,Foo>>(q => q.id, b => b.id, (f, b) => new Tuple<Bar,Foo>(b,f));
			ConsoleOut(queryJoinById);
			Console.WriteLine("-------------");
			
			Console.WriteLine("by Col string AND Id int");
			
			JoinCond[] conds2 = new JoinCond[2];
			conds2[0] = new JoinCond() { T1Property = "id", T2Property="id" };
			conds2[1] = new JoinCond() { T1Property = "ColBar", T2Property="ColFoo" };
			var w = MultiJoin<Bar,Foo>(conds2);
			
			ConsoleOut(w); //.Select(j => new Tuple<Bar,Foo>(j.t1,j.t2))
			
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
	}
}