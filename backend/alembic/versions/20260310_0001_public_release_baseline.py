"""public release baseline schema"""

from __future__ import annotations

import base64
import zlib

from alembic import op

revision = "20260310_0001"
down_revision = None
branch_labels = None
depends_on = None

# Frozen `pg_dump --schema-only` snapshot of the current PostgreSQL schema.
COMPRESSED_SCHEMA_B85 = (
    "c-rk<-",
    "EZ4C5`XVsA?(YhKyb)MlWTFfhg|FK2G>pRk`%XnB4mk*Rm+lAA5Oe~{gL{zNDe7dvSd5hKD4Mc<orYqhcm<B=;QtD=5c1tet(?ZJ",
    "=}i1vu;0IcV8dL54R7G57rtxS(I4+yM6rCdieP3?6(^$&gX&e{Nv4=k7d)xn}2<oRlokt4%26xpfyfXKMGUp<c&rCWH_|L4Ee5==",
    "j1Qi|J}DQUrvjMj-6u3qd#(-W~86c_7jYf=vmgq!p=~(NU-fnC=#3{K3>}aO7c)bgI}<dQ+92d^3f`Cae(}A9XWPE!91<r7KQn0j",
    "uXodGyFvUWqYqiCc`h8Rs6+1PBS~pQ~D-",
    "}?PR3&+D<lp_;m9AEk{{k0SM18&Un%|iB@q|&j!%u`W@dS2v*3k<18lwu8)bfN^mh<+c#`KN@F4on13djO)`%ec9Bg;{T1obUd7h",
    "4pDl|2T7O0%wm!{1-+cS>Xob=92@fWZ-Jb2=yp@S{lKCDP12oV5z=g$Hl;-jm0*3s?ktH~eP#W5Cx`<>8gGeC-",
    "+(ni~dE#KSvP0ja<IlpW#&W(<2tEN5*bb$lZ^w49^g|br08!TTg(dc59QYwdL_9q|z$moIw7>#g$w}zq7vw~t=Rcu^oi1Qeu3nkQ",
    "JXxf3Kk&1Sf|_5S!BdnFAz3Mw%N9`<Eph1oNfxy4VlqwHMw48*7OU~GlT^3kidLgnOmvg$lAn-DfmA96XEqgE^dlvm7JspWbu<fi",
    "o(D@rge*WmqB+8AGNeN{rEQnB#_Oab8`pMoVU|~c2xXfXTmaW^MU@a1q5&<*tE*=E($s#!Ek*kpC$t*<-",
    "P#z&mH}CSbOt$5sP$^`W2t`ga!rF8T&6lNF8L940^47~^WUs=Jt5h?6V(Swg-",
    "Us<XR&#YB-0^WU_v-2veixM`JtaKbUJnH(7}yZXyPTikBc*6<Z5v!Hz3&>O7qo<jGgp3PLe30>})?cMk7d~(5BL-ibbgFC$QcL+l",
    "QTHm>SDUBVM{v&(HY@aDDqJj0h|zeYFx%%xZ1L`DFa`Wua+hDlT?t2b<JS3x2SYdoLKLKgCh%QwjM=GO?wn1UY7<%s@{HbwG?XM{",
    "n<T{LC)b3m^$$l&tK4SctCFLu?GVdh|=*{N()V^7`!&3(ipeQTacy>!CajB54R9bryJK8#%+3|4h9%lV%1UdbMYO5rOi(R2~xD%@",
    "wV*eV@P%pK@w3>d5gx_pQ-|JzP814QKWD6DCGar<`&+I&$(AG2f2bA})eomB~L<z)c-%ivma<KD-",
    "t6XGRBKs&wIv8Vuzf<D8X)I9ZHz8wpalBAK5BnwsWl1{<%-",
    "T^bRswXBC5maraC<85@VvPS@#;~A>9jcpd@Yzr0Z8t37DVxO|m<%M?WQczh0dJ?(0Q%4sAyaspri+5cuB~G{KN0@K7O$*Ik)o2T@",
    "Cq}#PIM3t`P?TrN=S5_C!`lpGzrYD`15mMpykyemIzE_o&_X{FaqMm#eu<+bqY=|7f@{?pR@&~RoNzRqVS(yBWJ~ROfF2;*?8=T~",
    "8cvr9R%guh*u`Ywr^NsC9l8*E1p=fU{N?z+R>xm5<$8K1@Tol!+s=}>I)8cjqk8#$-09SR3hg3uz+@b^t@Ms4rOiDb2d-",
    "kHZwRG17$Oq+l6w`ggZ6S{R+rX-<T4w$r_WKcoJY}8y_(Y?f6|-",
    "is)7C@F_QOI6tUl0Y+7osN7^Eq{~(fumKZwbzdFBw+N5N+AZ~m{CP}iV?Jq)ijFMl8v6jdih=sOb_t>(E0R?SdRfv=rtVu+p8Tz?",
    "8$2gyV*fb0_nFizR5iQfUr$!rOED+xn_h3d{9rc;$mQI>-Fhn!g#HeL<yAhdnOe{C-",
    "_{E~(Om}lZuR{rm{S27~wIeHgUWF4SPc@8uv#q0t%6O$-M7>6Lu-",
    "u{O!X!q=H0ybE346`k%Ze_aBwhHimY^w4SR|)o*T*|FTyDJCu>lyq<3P&-",
    "`ImqvPtST1*{T4vV(7*Z+1HSS$2Abhuk*{Q7gwj*k>7K&B(!MB+!Cvs`oGxoq9g-",
    "|FE8qhV!Vo}gHF6vVK)R5e!G~DnbDlOOC7R0|0}jPg(lWkR(wO*CR@`rMzXfw)!bm{#wKaNU43YtYhdAoyJ^}fPOB)Rw>TmqBV>;",
    "Zur0(yaI*HvUXuBw2gTduG`WCx3|~)DG4-",
    "mQMIJ7Po_8n@F=9G~N>jSPfC`6Oo`9HSDDpgF#z;trN>dXQT&P~?Wj>n3=606cQx1%_K2T(qye2c4ifz1ute=5WO9r`^5xL$iJS*",
    "+lJD%|`PV_UBC3&_mP#`5e*{!3*mELIV(gX?_H}XNu+=3o9{;OJX%#4>as5RwvLz}#lp1Wp{i8YVh&0skg=8KXkg?AF54KZ#h`I<",
    "0Gq7`+@v6Lc<is#HdTfN}=VP>iJSU+Y9^k|lnkheHtuE_y~LiZhaOz_&r&vJ1TU7=EhsW2##bOW2XS;wB>!bsGpM%L>J1Rl)<Gnu",
    "IM1V8y1(e$eo`6pf2S}#nSxPFptByM*3-19oOj)stn7Bp$FuWuh<a;y)hV2Jv*VY2CrF;$8kii=bUM>eq`X~}^NB32}s%wvwAY-",
    "f?lhaWnhS_$~27L){V&p$}Crn0lZp2K<FQYCC)uHzR6CveVTXGkq|qNMZqGqJ;^#6me9f%0C8lXE=@(z@HCze9GP`b~0gj!)$0oR",
    "U%V{N))YPM#F^I%xC>3e~mK9f0R@OY?~LFz&WWEwu55X!y{`cADGHl16ZY$XRwy5<^S}Tq&)@+omBS&AwUc&d%)sujamk7Gz6Rb?",
    "eT_aD;A@bP~zNY|Ar#Dnm&+XG>8kUvKZXG<M=F)U|oaWS_dgnLN2aT$4cyi3W}ndcLS_xbY38JHhKU3AtNcW(Uf$!{WaS`P=oSG3",
    "^0tS{(g>S2>jRz{>Wyv$Ot5jYl&SrC)Ars2WyBBX<6iPp_;Qy7(*X`nI$_6u`$YpV%-K(3yE$9(G%l#y&PdAeLh-",
    "26#n7R#5uInONr~E$%4lFQS7Sk()1%v-@g5zTSQLE-",
    "C0)u)6v5$@=*9?&0zN=JxK<ikFBl`}v>y+uv^Pzgz#FeYZ}0SIK}attex_6<{=A0F{7~Z~?2u^<q^QC+lR?R59zvhAG6<&5W(vWW",
    "Z9q;3r`Q+yv~&Mu&r0=j}onvd)=e8m66B6&+N4*F_%;x@PMaNIK_=9GG;^-1(Pv&lUzS>b|*5EbV)H-",
    "PoXG<@HkuEA^Gt;mi&B_GZc6)zV<fUGD0~w!SFS&9Avq(~nVggQJ^Iebu8Mn<jU>o6$DS+<;XpX;{LcomOnpF`sfP>t0CBHDIz$i",
    "j(x-%9=CmHdaXOHmS*^Qy>#Z)<2ZA)0<V50;0vMP8Ts?CLWSYI02{Wec5QSFzTK?W|MT!oy8e--#$T-",
    "cF!C$_3EBI+UR7&orIXMtb^Y&9!+rEq*FdfDeGJaW*RWzLpKs0LWIYFMHRb{^jt?a3_7eL0Fn;t_<%`=)RUBTS9qBw{!zCBmjA&!",
    "U<f=2{dRZz-",
    ")}SP1bTSNeZ^o`{s+M(BS&E6*BT`B@YI5#v$b1?`!NKxNGM$7tdVlRp9+;q26;RjmI?;Xp0X9|SPb(j2xpcE$oQ#22v1Ih!e~3S<",
    "q0-r8>F~^TVs}!m`$OHFCfRABJn<PGsXOq-T4U|AhrQ&U6`z~Y=1g!*Kn8$5mz*!M#cU-",
    "6`1w~@fu0%w;i}uEdQDkZNt<x+vb;3%V0Vbjuy+gBJ6tOQ#J*17k(Ivqap1QO+KS;fS*PXv?$bLU!@=P_p1oAEkUc`S<S2Y<+Ob>",
    "<FtKYpttu`@h+Zgv)+YUMi~R7h`%V&3^@C{_S@^ZQv{7jh=lZF*qSn01KoZXk43uu5A(kW8<q%hS&%h1_UF@W=Hn@w<hW}%>}hw6",
    "hWT|@K3u^ZCPzX(T8)H%a9Si`4R*&U7M^bCSm?NDgfCBq*z@w5E%R$nw~cVCo|fgHnpf)B$P|tn>9TqXSqplWg4B(pozrKiJSMDr",
    "E&kU!UA(>fH2dB1U*r}aRQk+cdtP8?1W9Mjyl)S;cfVM5gOkqkWn|^1G+!)=)3@a({Z0bXclytt&(F?&?m%8!0Gdea$Sj9(XUJNc",
    "qzX|hMmYhJ$`GKlq{1-I5{wzMp{2wX-B%kh-F!8=mv7es3uy0Ejw%#G-",
    "PQOG2!wdTNp!tkwnAo~zuwPof4M7TFZIA%_p{Hl``O*c*@F;-",
    "8y=v6a4{L^?AaiTkRgPCw%d+^mCuFePXigeJTZirZ9fWT;ESEWaR`GPXaa#3dKm>j6zGDY{LC9bji6T$`x)gOc%z69!ZX-",
    ";VsJ?^?MK1fDCH)S%%E%@L-48=!xLUSA>m~TJ=PBw|AA4ZauBG8YlmK_w~+$y)SAJ(rG>x(%|}5je4|xJiQ-lhM-",
    "E}<dKe2g^RWm1oR3G%5W_m4-",
    "F_U*;>Ra|Q!y5~*aQU6_c988@v9i(fg6KfYyg4h`xpiL`1)5jAJBvI0l`x%D4~zR4K;#ZLF{L`x8c50TRytFaNkQ`g=^9ALd@ac$",
    "geg2b~zN`l!C6xM)ja<)JezB9Nm5$k|D^2&jojC4xz=4)rR>y@Bwc=Gf3f*Xt*O|B8NW6TxDc%c8Ev^0WX@4h<8#Q(V5EO$y3S2%",
    "mCp^0E1j02hCHqhl$=qvAv_q>+7q@_w`|~GpQmUg!EvP`k=Q^#BC5hyRs(=*zD?c#@9f4`99uKnvpfCa1TbeO)BCJZ$zY9QZUNfZ",
    "apIEX=hvqSc-&-YU<#JKt4S9VUBrP+0?@XeJZ<2aD^fQ3?*m*m5q0fbOI^tx(()Wx4{m;#xch{ffO2JI;^7&bNu@~)-cC=h-",
    "*pb<a1Yto?TZnJ@#tmRBOfrkx<_V@>)fzzCNodz5`s*2I$9MAKkh3-m){-e)8R>59<Bu#y*JoYYOrUT&#h_-",
    ")>N1XL}laA=~3GWKW>8`k-RI0I&4t8Op)Vd>^d)J*e!3_+XAH`=CFJL&`qLC)h7~pi&qH1FeCM3JxAq!C?|jWP)eEVMQ~n{AccA-",
    "<+rF@0XjQ+wZxL8P-Y-=tVguAIb4Gi1CF=Z`tcYCHinV=VIFOYn2`OQzjA4-M`=7=EM*#a8Gngiaxm4+;oU{M>}SLz2i91J54+K-",
    "S<AdrQWsJ>C*>y`5wnL>aBaGaA_U}eES}*df=N5c2fz-",
    "11ljNz!}Cc{9Hlk?lj*WT2aH0Pb%e#8`U0Ex=N+Z%O!xWocV`6>2%|_IS|cgj9YHBRFBZL<2d^@bc|o>um%5s)VZ<%fw$}`K(JR)",
    "?}dF*$>@c!!*kn#%hpl#!CS&rA4VH7Gd$ykd$fuRdO&f(2|_>pvF$eu)F1cwa;L6G;<f8M9)J;Rf_8j)G(mG%GdaF~RhF9ZT7^C*",
    "nkPLh)69|{mI>xavp|oxloaS0f3$`oWJ?7}O0u%3*(MNnu$NsGj`bvkV-G?54t|I6B}T8vlLytzMe=W(*1vd{?=HDvmbMw5f-",
    "&S>#@DJt(CzmyX$bO(C0GmD<g=}b>M+M@{C%~%B(=wvq@G{~d!gD(oti#K_v_*HL3~(glYQP`{Pk^8xzCeVU!Qb!4MV!$6v|z_#R",
    "QU}Dw6NDisU2klcZ|f8Ei7#em1~7amu$LvOOIH8lVKeQrj`*K#M77eEb!oQ~A>qKh7|L?(B<f{4rD181<TuLrgG2dN_7A9{&evCE",
    "<$",
)

TABLE_NAMES = (
    "answer_versions",
    "artifact_builds",
    "bulk_fill_job_events",
    "bulk_fill_requests",
    "bulk_fill_row_executions",
    "case_profile_items",
    "case_profiles",
    "chat_messages",
    "chat_threads",
    "evidence_links",
    "execution_runs",
    "export_jobs",
    "historical_case_profile_items",
    "historical_case_profiles",
    "historical_client_packages",
    "historical_datasets",
    "historical_qa_rows",
    "historical_workbooks",
    "memberships",
    "model_invocations",
    "pdf_chunks",
    "pdf_pages",
    "product_truth_chunks",
    "product_truth_records",
    "questionnaire_rows",
    "questionnaires",
    "repo_snapshots",
    "retrieval_runs",
    "retrieval_snapshot_items",
    "rfx_cases",
    "runtime_snapshots",
    "source_manifests",
    "tenants",
    "uploads",
    "users",
)


def _schema_statements() -> list[str]:
    payload = zlib.decompress(base64.b85decode("".join(COMPRESSED_SCHEMA_B85))).decode("utf-8")
    return [statement.strip() for statement in payload.split("\n\n") if statement.strip()]


def _statement_preview(statement: str, *, max_length: int = 120) -> str:
    preview = " ".join(statement.splitlines())
    if len(preview) <= max_length:
        return preview
    return preview[: max_length - 3] + "..."


def upgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("The first public release baseline only supports PostgreSQL.")
    statements = _schema_statements()
    total = len(statements)
    print(f"[alembic] Applying public release baseline statements={total}", flush=True)
    for index, statement in enumerate(statements, start=1):
        print(
            f"[alembic] Statement {index}/{total}: {_statement_preview(statement)}",
            flush=True,
        )
        bind.exec_driver_sql(statement)
    print(f"[alembic] Public release baseline complete statements={total}", flush=True)


def downgrade() -> None:
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        raise RuntimeError("The first public release baseline only supports PostgreSQL.")
    for table_name in reversed(TABLE_NAMES):
        bind.exec_driver_sql(f"DROP TABLE IF EXISTS public.{table_name} CASCADE")
    bind.exec_driver_sql("DROP EXTENSION IF EXISTS vector")
