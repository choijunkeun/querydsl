package study.querydsl;


import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceUnit;
import org.aspectj.lang.annotation.Before;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import java.util.List;

import static com.querydsl.jpa.JPAExpressions.select;
import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.member;
import static study.querydsl.entity.QTeam.team;

@SpringBootTest
@Transactional
public class QueryDslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;   // 필드로 빼도 동시성문제 없음.

    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);

        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }


    // JPQL
    @Test
    public void startJPQL() {
        // member1을 찾아라
        String qlString =
                "select m from Member m " +
                "where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void startQueryDsl() {
//        QMember m = new QMember("m"); //변수명에 별칭 주는것. 같은테이블 조인 할 때 선언해서 사용하고 나머진 스태틱임포트를 사용하자.
//        QMember m = QMember.member;     // 방법2
        // 방법3은 static import 사용

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))    // 파라미터 바인딩 처리
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {  // 보통 이걸로 많이 씀
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
        // List
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        // 단 건
        Member fetchOne = queryFactory
                .selectFrom(member)
                .fetchOne();

        // 처음 한 건 조회(limit 1)
        Member fetchFirst = queryFactory
                .selectFrom(member)
                .fetchFirst();

        // 페이징에서 사용(fetchResult(), fetchCount()는 deprecate되었음)
        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults();

        results.getTotal();
        List<Member> content = results.getResults();

        // 카운트 쿼리(디플리케이트 되었으므로 아래 코드 참조)
        long total = queryFactory
                .selectFrom(member)
                .fetchCount();

        // 카운트쿼리 (별도로 이처럼 작성해야 함)
        Long totalCount = queryFactory
                .select(member.count())
                .from(member)
                .fetchOne();    // 응답결과는 숫자 하나이므로 fetchOne()을 사용

        System.out.println("totalCount = " + totalCount);
    }


    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     * 단, 2에서 회원 이름이 없으면 마지막에 출력 (nulls last)
     */
    @Test
    public void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();

    }

    @Test
    public void paging1() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        QueryResults<Member> queryResults = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults()).size().isEqualTo(2);
    }

    @Test
    public void aggregation() { // 집합함수
        List<Tuple> result = queryFactory   // 데이터 타입이 여러개일 때 Tuple (실무에선 많이 안쓰고 Dto로 뽑아옴)
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch();


        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구하라
     * */
    @Test
    public void group() throws Exception {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }


    /**
     * 팀 A에 소속된 모든 회원
     * */
    @Test   // join 기본조인
    public void join() throws Exception {
        List<Member> result = queryFactory
                .selectFrom(member)
                .leftJoin(member.team, team)    // join, leftJoin, rightJoin 다 가능
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인(연관관계 없는 조인)
     * 세타 조인은 외부 조인 불가능(left,right) -> 조인 on을 사용하면 외부 조인 가능
     * 회원의 이름이 팀 이름과 같은 회원 조회
     * */
    @Test
    public void theta_join() throws Exception {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .where(member.username.eq(team.name))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * on 절 예제..
     * 회원과 팀을 조인하면서, 팀 이름이 TEAmA인 팀만 조인, 회원은 모두 조회
     * JPQL : select m , t from Member m left join m.team t on t.name = 'teamA'
     * */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team)
                .on(team.name.eq("teamA"))
                .fetch();

        /*
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .Join(member.team, team)
                .where(team.name.eq("teamA") // inner join이면 on 대신 where 절로 사용. 결과 같음
                .fetch();
         */

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 연관관계가 없는 엔티티 외부조인
     * 회원의 이름이 팀 이름과 같은 대상 외부 조인
     * */
    @Test
    public void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

        /**
         * 서로 관계없는 필드로 외부 조인 할 경우,
         * 일반조인 : from(member).leftJoin(member.team, team)
         * on 조인 : from(member).leftJoin(team).on(xxx)
         * */
    }


    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() throws Exception {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());//초기화된지 안된애인지 확인해줌
        assertThat(loaded).as("패치 조인 미적용").isFalse();
    }

    @Test
    public void fetchJoinUse() throws Exception {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());//초기화된지 안된애인지 확인해줌
        assertThat(loaded).as("패치 조인 적용").isTrue();
    }


    /**
     * 나이가 가장 많은 회원 조회 (서브쿼리 이용)
     * */
    @Test
    public void subQuery() throws Exception {
        // alias가 겹치면 안되므로 서브쿼리용 QMember를 만들어줌
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * 나이가 평균 이상인 회원
     * */
    @Test
    public void subQueryGoe() throws Exception {
        // alias가 겹치면 안되므로 서브쿼리용 QMember를 만들어줌
        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30,40);
    }

    /**
     * 서브쿼리에서 in 적용
     * */
    @Test
    public void subQueryIn() throws Exception {
        // alias가 겹치면 안되므로 서브쿼리용 QMember를 만들어줌
        QMember memberSub = new QMember("memberSub");

        // 실습을 위해 작성한 쿼리지만 아래는 효율적이진 않은 쿼리이다.
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))    // 10살 초과
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(20,30,40);
    }

    /**
     * select절에서 서브쿼리 사용
     * */
    @Test
    public void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }

        /**
         * from절의 서브쿼리 한계
         * JPA JPQL 서브쿼리의 한계는 from절에서 서브쿼리(인라인 뷰)를 지원하지 않는다. QueryDsl도 마찬가지.
         * ######
         * 하이버네이트6.1부터는 from절에서 서브쿼리를 지원한다. 그러나 공식적으로 지원해주지는 않고 있는것으로 보여
         * 서드파티를 적용해야 할 것 같다.
         * ######
         *
         * 해결방안.
         * 1. 서브쿼리를 join으로 변경
         * 2. 애플리케이션에서 쿼리를 2번 분리해서 실행
         * 3. nativeSQL 사용
         * */
    }


    /**
     * case문
     * (가급적 이런 문제는 DB에서 해결하지 말자)
     * db는 row데이터를 필터링하고 그룹핑하거나 필요하면 계산할 수 있지만
     * 최소한의 필터링과 그룹핑을 해서 데이터를 줄이는 일만 하고
     * 10살이야 20살이야 보여주는건 DB에서 하지말고 그냥 가져와서 애플리케이션 또는 프레젠테이션 레이어에서 해야한다.
     * */
    @Test
    public void basicCase() {
        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }
    @Test
    public void complexCase() {
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21살~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    /**
     * 상수 더하기
     * */
    @Test
    public void constant() {
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 문자 더하기
     * */
    @Test
    public void concat() {

        // {username}_{age}
        // 타입이 다르기 떄문에 stringValue로 문자열로 바꿔준다.
        // .stringValue() 생각보다 쓸일이 많다 -> Enum타입일 때 Enum이라 값이 안나오는데 그럴 때 사용
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }





}
