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

    [Table(Name = "qux")]
    public class Qux
    {
        [PrimaryKey, Column(Name = "id"), NotNull]
        public int id { get; set; }

        [Column(Name = "success"), NotNull]
        public bool Success { get; set; }

    }



    public class MyContext : LinqToDB.Data.DataConnection
	{
		public MyContext() : base("SQLite","Data Source=mydb.s3db") { }
		
	}
	
	
	//static class ExpressionTestExtensions
	//{
		
	//	#region with LeftJoinInfo
	//	/*
	//	[ExpressionMethod("LeftJoinImpl")]
 //		public class LeftJoinInfo<TOuter,TInner>
	//	{
	//		public TOuter Outer;
	//		public TInner Inner;
	//	}

	//	public static IQueryable<LeftJoinInfo<TOuter,TInner>> LeftJoin<TOuter, TInner, TKey>(
	//		this IQueryable<TOuter> outer,
	//		IEnumerable<TInner> inner,
	//		Expression<Func<TOuter, TKey>> outerKeySelector,
	//		Expression<Func<TInner, TKey>> innerKeySelector)
	//	{
	//		return outer
	//			.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
	//			.SelectMany(t => t.gr//.DefaultIfEmpty()
	//			            , (o,i) => new LeftJoinInfo<TOuter,TInner> { Outer = o.o, Inner = i });
	//	}

	//	static Expression<Func<
	//		IQueryable<TOuter>,
	//		IEnumerable<TInner>,
	//		Expression<Func<TOuter,TKey>>,
	//		Expression<Func<TInner,TKey>>,
	//		IQueryable<LeftJoinInfo<TOuter,TInner>>>>
	//		LeftJoinImpl<TOuter, TInner, TKey>()
	//	{
	//		return (outer,inner,outerKeySelector,innerKeySelector) => outer
	//			.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
	//			.SelectMany(t => t.gr//.DefaultIfEmpty()
	//			            , (o,i) => new LeftJoinInfo<TOuter,TInner> { Outer = o.o, Inner = i });
	//	}*/
	//	#endregion

	//	[ExpressionMethod("InnerJoinImpl")]
	//	public static IEnumerable<TResult> InnerJoin<TOuter,TInner,TKey,TResult>(
	//		this IQueryable<TOuter> outer,
	//		IEnumerable<TInner> inner,
	//		Expression<Func<TOuter,TKey>> outerKeySelector,
	//		Expression<Func<TInner,TKey>> innerKeySelector,
	//		Expression<Func<TOuter,TInner,TResult>> resultSelector)
	//	{
	//		return outer
	//			.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
	//			.SelectMany(t => t.gr//.DefaultIfEmpty()
	//			            , (o,i) => resultSelector.Compile()(o.o,i));
	//	}
	//	static Expression<Func<
	//		IQueryable<TOuter>,
	//	IEnumerable<TInner>,
	//	Expression<Func<TOuter,TKey>>,
	//	Expression<Func<TInner,TKey>>,
	//	Expression<Func<TOuter,TInner, TResult>>,
	//	IQueryable<TResult>>>
	//		LeftJoinImpl1<TOuter,TInner,TKey,TResult>()
	//	{
	//		return (outer,inner,outerKeySelector,innerKeySelector,resultSelector) => outer
	//			.GroupJoin(inner, outerKeySelector, innerKeySelector, (o, gr) => new { o, gr })
	//			.SelectMany(t => t.gr//.DefaultIfEmpty()
	//			            , (o,i) => resultSelector.Compile()(o.o,i));
	//		//.Select<TResult>((o,i) => resultSelector.Compile()(o,i));
	//	}
		
	//}
	
	
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

		
		//static internal IEnumerable<TResult>
		//	NewJoin<T1, T2, TKey, TResult>
		//	(Expression<Func<T2, TKey>> outer, Expression<Func<T1, TKey>> inner, Expression<Func<T2,T1,TResult>> resultSelector)
		//	where T2: class
		//	where T1 : class
		//{

		//	using (var db = new MyContext()) {

		//		var query = (from b in db.GetTable<T2>() select b).InnerJoin <T2,T1, TKey, TResult>((from f in db.GetTable<T1>() select f), outer, inner, resultSelector);//.AsEnumerable();
				
		//		#region only for debugging purposes - to be deleted
		//		//Console.WriteLine(String.Format("Last Query: {0}",db.LastQuery));
		//		#endregion
				
		//		return query;

		//	}
		//}
		
		
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
//		public class JoinCond<T1,T2>
//		{
//			public JoinCond(Expression<Func<JoinClass<T1,T2>, bool>> JoinCond) {
//				myExpr = JoinCond;
//			}
//			#region equal expr
////			public JoinCond(Expression<Func<T1, object>> T1_Property, Expression<Func<T2, object>> T2_Property)
////			{
////				if (T1_Property.Body.NodeType.Equals(ExpressionType.Convert))
////				{
////					T1Property = ((T1_Property.Body as
////					               UnaryExpression).Operand as MemberExpression).Member.Name;
////				}
////				else
////				{
////					T1Property = (T1_Property.Body as
////					              MemberExpression).Member.Name;
////				}
////				if (T2_Property.Body.NodeType.Equals(ExpressionType.Convert))
////				{
////					T2Property = ((T2_Property.Body as
////					               UnaryExpression).Operand as MemberExpression).Member.Name;
////				}
////				else
////				{
////					T2Property = (T2_Property.Body as
////					              MemberExpression).Member.Name;
////				}
////			}
//			#endregion
//			public string T1Property;
//			public string T2Property;
//			public Expression<Func<JoinClass<T1,T2>, bool>> myExpr = null;
//		}
		public static IQueryable<JoinClass<T1,T2>> MultiJoin<T1,T2> (
            MyContext db,
            Expression<Func<JoinClass<T1, T2>, bool>> JoinCond
        )
			where T2: class
			where T1 : class
		{
			var jfb = Expression.Parameter(typeof(JoinClass<T1, T2>), "jfb");

			FieldInfo ItemT1 = typeof(JoinClass<T1, T2>).GetField("t1");
			FieldInfo ItemT2 = typeof(JoinClass<T1, T2>).GetField("t2");

			var pb = Expression.Field(jfb, ItemT1);//
			var pf = Expression.Field(jfb, ItemT2);//

            var old_expr = JoinCond;
            var map = old_expr.Parameters.ToDictionary(p => p, p => jfb);
            var reboundBody = ParameterRebinder.ReplaceParameters(map, old_expr.Body);
            var newCond = Expression.Lambda<Func<JoinClass<T1, T2>, bool>>(reboundBody, jfb).Body;

            var WhereExpr = Expression.Lambda<Func<JoinClass<T1,T2>,bool>>(newCond, jfb);
			
			//using (var db = new MyContext()) {

                var qx = db.GetTable<T1>()
                        .SelectMany(
                        b => db.GetTable<T2>()
                            , (b,f) => new JoinClass<T1, T2>() { t1 = b, t2 = f });

                //Translated Above from Query (or Query Expression) to Method Chaining (or Fluent)
                //var qxExe = qx.ToArray();
                //Console.WriteLine(String.Format("Last QX: {0}", db.LastQuery));

                //var q = from b in db.GetTable<T1>()
                //        from f in db.GetTable<T2>()
                //        select new JoinClass<T1, T2>() { t1 = b, t2 = f };

                var w = qx.Where(WhereExpr);
				
				#region only for debugging purposes - to be deleted
				//w.ToArray();
				//Console.WriteLine(String.Format("Last Query: {0}",db.LastQuery));
				#endregion
				
				return w;
			//}
		}
        public class JoinClass<T1, T2, T3>
        {
            public T1 t1;
            public T2 t2;
            public T3 t3;
        }
        public static IQueryable<JoinClass<T1, T2, T3>> MultiJoin<T1, T2, T3>(
            MyContext db,
            Expression<Func<JoinClass<T1, T2, T3>, bool>> JoinCond
        )
            where T3 : class
            where T2 : class
            where T1 : class
        {
            var jfb = Expression.Parameter(typeof(JoinClass<T1, T2, T3>), "jfb");

            FieldInfo ItemT1 = typeof(JoinClass<T1, T2, T3>).GetField("t1");
            FieldInfo ItemT2 = typeof(JoinClass<T1, T2, T3>).GetField("t2");
            FieldInfo ItemT3 = typeof(JoinClass<T1, T2, T3>).GetField("t3");

            var pb = Expression.Field(jfb, ItemT1);//
            var pf = Expression.Field(jfb, ItemT2);//
            var pq = Expression.Field(jfb, ItemT3);//

            var old_expr = JoinCond;
            var map = old_expr.Parameters.ToDictionary(p => p, p => jfb);
            var reboundBody = ParameterRebinder.ReplaceParameters(map, old_expr.Body);
            var newCond = Expression.Lambda<Func<JoinClass<T1, T2, T3>, bool>>(reboundBody, jfb).Body;

            var WhereExpr = Expression.Lambda<Func<JoinClass<T1, T2, T3>, bool>>(newCond, jfb);

            //using (var db = new MyContext()) {

            var qx = db.GetTable<T1>()
                    .SelectMany(
                    b => db.GetTable<T2>(), (b, f) => new JoinClass<T1, T2>() { t1 = b, t2 = f })
                    .SelectMany(
                    q => db.GetTable<T3>()
                        , (j, q) => new JoinClass<T1, T2, T3>() { t1 = j.t1, t2 = j.t2, t3 = q });

            //Translated Above from Query (or Query Expression) to Method Chaining (or Fluent)
            //var qxExe = qx.ToArray();
            //Console.WriteLine(String.Format("Last QX: {0}", db.LastQuery));

            //var q = from b in db.GetTable<T1>()
            //        from f in db.GetTable<T2>()
            //        select new JoinClass<T1, T2>() { t1 = b, t2 = f };

            var w = qx.Where(WhereExpr);

            #region only for debugging purposes - to be deleted
            //w.ToArray();
            //Console.WriteLine(String.Format("Last Query: {0}",db.LastQuery));
            #endregion

            return w;
            //}
        }

        public static void Main(string[] args)
		{
			Console.WriteLine("Hello World!");


            
            Console.WriteLine("by Id int");
            
            using(var db = new MyContext()) { 

            var queryJoinById = MultiJoin<Bar, Foo>(db, j => j.t1.id == j.t2.id);
            //Test.NewJoin<Bar, Foo, int, Tuple<Bar,Foo>>(q => q.id, b => b.id, (f, b) => new Tuple<Bar,Foo>(b,f));
            Console.WriteLine(String.Format("Last Query: {0}", db.LastQuery));
            ConsoleOut(queryJoinById);
            Console.WriteLine("-------------");

            Console.WriteLine("by Col string");
			var queryJoinByCol = MultiJoin<Bar,Foo>(db, j => j.t2.ColFoo.StartsWith(j.t1.ColBar));
			//Test.NewJoin<Bar, Foo, string, Tuple<Bar,Foo>>(q => q.ColFoo, b => b.ColBar, (f, b) => new Tuple<Bar,Foo>(b,f));
			Console.WriteLine(String.Format("Last Query: {0}", db.LastQuery));
            ConsoleOut(queryJoinByCol);
			Console.WriteLine("-------------");
			
			
			Console.WriteLine("by Col string AND Id int");

			var w = MultiJoin<Bar,Foo>(db, 
                j => j.t2.ColFoo.StartsWith(j.t1.ColBar) && j.t1.id == j.t2.id
                );
            Console.WriteLine(String.Format("Last Query: {0}", db.LastQuery));
            ConsoleOut(w); //.Select(j => new Tuple<Bar,Foo>(j.t1,j.t2))

                var test = MultiJoin<Bar, Qux>(db,
                                j => j.t1.id == j.t2.id && !j.t2.Success
                                );

                Console.WriteLine(String.Join(",",
                        test.Select(t => t.t1.Name)
                    )
                    );
                Console.WriteLine(String.Format("Last Query: {0}", db.LastQuery));

                var complex = MultiJoin<Bar, Foo, Qux>(db,
                    j => j.t2.ColFoo.StartsWith( j.t1.ColBar) && j.t2.id == j.t3.id && j.t3.Success
                    );
                Console.WriteLine(String.Join(",",
                        complex.Select(t => t.t1.Name + "@" + t.t2.FromDate.ToShortDateString())
                    )
                    );
                Console.WriteLine(String.Format("Last Query: {0}", db.LastQuery));

            }



            #region SO try to generalize the Equal Expr into LambdaExpr in the generic JoinCond
            //I have this expression
            Expression<Func<Bar,bool>> old_expr = x => x.Name == x.ColBar;
			//I want to change parameter from x to y
			//I already have the y parameter in the code, let's say it is the followinf
			ParameterExpression py = Expression.Parameter(typeof(Bar), "y");
			//this is what I have tried, but it is *not* complete neither generic
			Expression expr_to_do;
			if (old_expr.Body.NodeType.Equals(ExpressionType.Convert)) {
				UnaryExpression convEx = old_expr.Body as UnaryExpression;
				expr_to_do = convEx.Operand;
			} else {
				expr_to_do  = old_expr.Body;
			}
			//TODO convert the BinarayExpression eqEx, etc... etc...
			
			if (expr_to_do.NodeType.Equals(ExpressionType.Equal)) {
				// have I to manage each Expr Type case??
				var eqExpr = expr_to_do as BinaryExpression;
				var left = eqExpr.Left as MemberExpression;
				//...
				//var new_left = Expression.Property(...,py)
			}
			var newLambda = Expression.Lambda(expr_to_do, new ParameterExpression[1]{py});
			
			var map = old_expr.Parameters.ToDictionary(p => p, p => py);
			var reboundBody = ParameterRebinder.ReplaceParameters(map, old_expr.Body);
			var lambda = Expression.Lambda<Func<Bar,bool>>(reboundBody, py);
			
			//newLambda = Expression.Lambda(Expression.Call(
			//	old_expr.Compile().Method, new Expression[] {Expression.Constant(new Bar()),Expression.Constant(new Bar())})
			//                              ,new ParameterExpression[1]{py});
			
			//Again, what I want to get is the following where y *is* the parameter defined *above* 
			Expression<Func<Bar,bool>> new_expr = y => y.Name == y.ColBar;
			//The code/method I'm looking for - if it does exist a method to do that - must be generic enough not specific to this single expression
			#endregion
			
			Console.Write("Press any key to continue . . . ");
			Console.ReadKey(true);
		}
		
		
		public class ParameterRebinder : ExpressionVisitor
		{
			private readonly Dictionary<ParameterExpression, ParameterExpression> Map;

			public ParameterRebinder(Dictionary<ParameterExpression, ParameterExpression> map)
			{
				this.Map = map ?? new Dictionary<ParameterExpression, ParameterExpression>();
			}

			public static Expression ReplaceParameters(Dictionary<ParameterExpression, ParameterExpression> map, Expression exp)
			{
				return new ParameterRebinder(map).Visit(exp);
			}

			protected override Expression VisitParameter(ParameterExpression node)
			{
				ParameterExpression replacement;
				if (this.Map.TryGetValue(node, out replacement))
				{
					return replacement;
					//return this.Visit(replacement);
				}
				return base.VisitParameter(node);
			}
		}
		
	}
}