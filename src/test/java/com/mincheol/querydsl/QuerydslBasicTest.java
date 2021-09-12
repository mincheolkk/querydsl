package com.mincheol.querydsl;

import com.mincheol.querydsl.entity.Member;
import com.mincheol.querydsl.entity.QMember;
import com.mincheol.querydsl.entity.QTeam;
import com.mincheol.querydsl.entity.Team;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static com.mincheol.querydsl.entity.QMember.*;
import static com.mincheol.querydsl.entity.QTeam.team;
import static org.assertj.core.api.Assertions.*;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;

    JPAQueryFactory queryFactory;

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

    @Test
    public void startJPQL() {
        // member1 찾기
        String qlString = "select m from Member m " +
                "where m.username = :username";

        Member findMember = em.createQuery(qlString, Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    // JPQL 에서는 username 을 serParameter() 를 사용해, 파라미터 바인딩 해 줌.
    // Querydsl 은 파라미터 바인딩을 자동으로 함.
    // Querydsl 은 Q타입을 만들어서 자바코드로 풀어냄

    @Test
    public void startQuerydsl() {
//        JPAQueryFactory queryFactory = new JPAQueryFactory(em);  밖으로 빼서 필드레벌로 둬도 됌.

        // Q 클래스 인스턴스 사용하는 법 .
//        QMember m = new QMember("m");        // 1. 별칭 직접 지정
//        QMember qMember = QMember.member;    // 2. 기본 인스턴스 사용 ( 아래 member처럼 static 임포트 가능)

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void search() {
        Member findMember = queryFactory
                .selectFrom(member) // == select(member).from(member)
                .where(member.username.eq("member1")
                        .and(member.age.between(10, 30)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void searchAndParam() {
        Member findMember = queryFactory
                .selectFrom(member) // == select(member).from(member)
                .where(member.username.eq("member1")
                        , member.age.eq(10))   // 여러개를 넘기면 다 and 또한 여러 개 중에 null 이 있으면 null 무시함. 동적쿼리에 좋음 .
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    public void resultFetch() {
//        List<Member> fetch = queryFactory
//                .selectFrom(member)
//                .fetch();  // member 의 목록을 리스트로 조회
//
//        Member fetchOne = queryFactory
//                .selectFrom(QMember.member)
//                .fetchOne();
//
//        Member fetchFirst = queryFactory
//                .selectFrom(QMember.member)
//                .fetchFirst(); // == limit(1).fetchOne()

        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults();
        // 페이징 쿼리가 복잡해지면, 데이터를 가져오는 쿼리와 total count 를 가져오는 쿼리가 다를 수도 있음. 성능 때문에.
        // 복잡하고 성능이 중요한 페이징 쿼리에서는 이걸 쓰면 안 됨. 쿼리 두번을 따로 날리는게 나음.

        results.getTotal();
        List<Member> content = results.getResults();

        long total = queryFactory
                .selectFrom(member)
                .fetchCount();
    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 올림차순(asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력(nulls last)
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
                .orderBy(member.username.desc())  //orderBy 를 넣어야 페이징이 잘 작동하는지 확인할 수 있음
                .offset(1)  // 0 부터 시작. 1은 하나를 스킵한다는 뜻.
                .limit(2)
                .fetch();

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    public void paging2() {
        QueryResults<Member> queryResults = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())  //orderBy 를 넣어야 페이징이 잘 작동하는지 확인할 수 있음
                .offset(1)  // 0 부터 시작. 1은 하나를 스킵한다는 뜻.
                .limit(2)
                .fetchResults();

        assertThat(queryResults.getTotal()).isEqualTo(4);
        assertThat(queryResults.getLimit()).isEqualTo(2);
        assertThat(queryResults.getOffset()).isEqualTo(1);
        assertThat(queryResults.getResults().size()).isEqualTo(2);
    }

    @Test
    public void aggregation() {
        List<Tuple> result = queryFactory
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
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

        // 데이터 타입이 여러 개 일때 tuple 을 사용함. 실무에선 DTO 로 뽑아냄.
    }

    /**
     *  팀의 이름과 각 팀의 평균 연령을 구해라.
     */
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
     */
    @Test
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)  // join 과 innerjoin은 같음
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    // 연관관계 없어도 조인 할 수 있음
    /**
     *  세타 조인
     *  회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    public void theta_join() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team)         // 모든 회원, 모든 팀을 다 가져오고 이걸 다 조인함.
                .where(member.username.eq(team.name))   // 조인한 테이블에서, where 절로 필터링 ( 멤버 이름과 팀의 이름이 같은걸 찾음)
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");

        // 세타 조인은 left, outer 조인이 불가능 (외부 조인 불가능)
    }

    /**
     *  ex) 회원과 팀을 조인하면서, 팀 이름이 teamA 인 팀만 조인, 회원은 모두 조회
     *  JPQL : select m, t from Member m left join m.team on t.name = 'teamA'
     */
    @Test
    public void join_on_filtering() {
        List<Tuple> result = queryFactory
                .select(member, team)  // select 가 member, team 두 개의 다른 타입이니 결과는 Tuple
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA"))
                // leftjoin 이기 때문에 member 를 기준으로 member 데이터들은 모두 가져옴.
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
            // 결과. leftjoin 이기에 member3,4 도 나옴. join 일 때는 안 나옴.

//            tuple = [Member(id=3, username=member1, age=10), Team(id=1, name=teamA)]
//            tuple = [Member(id=4, username=member2, age=20), Team(id=1, name=teamA)]
//            tuple = [Member(id=5, username=member3, age=30), null]
//            tuple = [Member(id=6, username=member4, age=40), null]

            // 특히, join 일 때는 on 절 대신 where 절을 써도 동일하게 필터링 됨.
        }
    }

    /**
     *  연관 관계가 없는 엔티티 외주 조인
     *  회원의 이름이 팀 이름과 같은 대상을 외부 조인
     */
    @Test
    public void join_on_no_relation() {
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(team).on(member.username.eq(team.name))  // leftjoin() 부분에 일반조인과 다르게 엔티티 하나만 들어감.
                .fetch();

        // 일반 조인 : leftJoin(member.team, team)   member의 FK 값을 team 의 PK 와 연결.
        // on 조인 : from(member).leftJoin(team).on(xxx)


        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
            // soutv 결과
//            tuple = [Member(id=3, username=member1, age=10), null]
//            tuple = [Member(id=4, username=member2, age=20), null]
//            tuple = [Member(id=5, username=member3, age=30), null]
//            tuple = [Member(id=6, username=member4, age=40), null]
//            tuple = [Member(id=7, username=teamA, age=0), Team(id=1, name=teamA)]
//            tuple = [Member(id=8, username=teamB, age=0), Team(id=2, name=teamB)]
            // member.username 과 team.name 이 같은 경우에만 team을 조인함
//            tuple = [Member(id=9, username=teamC, age=0), null]

            // sql 일부
//            left outer join
//            team team1_
//            on (
//                    member0_.username=team1_.name
//            )

            /**
             *  기존 스타일로 leftjoin 일 때, sql 비교
             *  .leftJoin(member.team, team).on(member.username.eq(team.name))
             */
            // sql 결과
//            left outer join
//            team team1_
//            on member0_.team_id=team1_.team_id
//            and (
//                    member0_.username=team1_.name
//            )
            // soutv 결과
//            tuple = [Member(id=3, username=member1, age=10), null]
//            tuple = [Member(id=4, username=member2, age=20), null]
//            tuple = [Member(id=5, username=member3, age=30), null]
//            tuple = [Member(id=6, username=member4, age=40), null]
//            tuple = [Member(id=7, username=teamA, age=0), null]
//            tuple = [Member(id=8, username=teamB, age=0), null]
//            tuple = [Member(id=9, username=teamC, age=0), null]
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    public void fetchJoinNo() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        // 로딩된 엔티티인지, 초기화가 안 된 엔티티인지 알려줌

        assertThat(loaded).as("패치 조인 미적용").isFalse();
    }

    @Test
    public void fetchJoinUse() {
        em.flush();
        em.clear();

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member1"))
                .fetchOne();

        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());

        assertThat(loaded).as("패치 조인 적용").isTrue();
    }
    // fetch join 이라는 기능 자체의 핵심은 연관된 엔티티를 한번에 최적화해서 조회하는 기능.
    // 그래서 LAZY가 발생하지 않음.

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    public void subQuery() {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        // SQL 결과
//        /* select
//        member1
//    from
//        Member member1
//    where
//        member1.age = (
//            select
//                max(memberSub.age)
//            from
//                Member memberSub
//        ) */ select
//        member0_.member_id as member_i1_1_,
//                member0_.age as age2_1_,
//        member0_.team_id as team_id4_1_,
//                member0_.username as username3_1_
//        from
//        member member0_
//        where
//        member0_.age=(
//                select
//        max(member1_.age)
//        from
//        member member1_
//            )

        System.out.println("result = " + result);
        // result = [Member(id=6, username=member4, age=40)]

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * 나이가 평균 이상인 회원
     */
    @Test
    public void subQueryGoe() {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        System.out.println("result = " + result);
        // result = [Member(id=5, username=member3, age=30), Member(id=6, username=member4, age=40)]

        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    /**
     * 특정 나이 이상인 회원
     */
    @Test
    public void subQueryIn() {

        QMember memberSub = new QMember("memberSub");

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        System.out.println("result = " + result);
        // result = [Member(id=4, username=member2, age=20), Member(id=5, username=member3, age=30), Member(id=6, username=member4, age=40)]

        assertThat(result).extracting("age")
                .containsExactly(20, 30, 40);
    }

    @Test
    public void selectSubQuery() {

        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
//            tuple = [member1, 25.0]
//            tuple = [member2, 25.0]
//            tuple = [member3, 25.0]
//            tuple = [member4, 25.0]
        }
    }

    /**
     *  JPA JPQL 서브쿼리는 where,select 에서는 사용가능
     *  하지만 from 절에선 불가
     *  이를 위한 해결책
     *  1. from 절에서는 서브쿼리를 join으로 변경(불가능한 상황도 있음)
     *  2. 쿼리를 2번으로 분리해서 실행
     *  3. nativeSQL 을 사용.
     */
}

