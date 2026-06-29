import React, { useState, useRef, useEffect } from 'react';
import { motion, useScroll, AnimatePresence } from 'framer-motion';
import { ChevronsDown, Check, Fingerprint, AlertCircle, ExternalLink, Lock, ShieldAlert, Zap, RefreshCw, Database, Clock, Activity, ArrowLeft } from 'lucide-react';

const INK='#0A0E14', INK2='#0F151E', GREEN='#34E0A1', RED='#EF5B5B', GOLD='#E7C884',
  TXT='#E9ECF2', BODY='#BCC2CE', DIM='#8A93A3', HAIR='rgba(255,255,255,0.08)';
const SANS="'PingFang SC','Source Han Sans SC','Noto Sans SC',system-ui,-apple-system,sans-serif";
const SERIF="'Noto Serif SC','Songti SC','STSong','Source Han Serif SC',serif";
const MONO="'SF Mono','Roboto Mono',ui-monospace,Menlo,monospace";
const EASE=[0.22,1,0.36,1];

const smooth=(p)=>{if(p.length<2)return'';let d=`M ${p[0][0]} ${p[0][1]}`;for(let i=0;i<p.length-1;i++){const a=p[i-1]||p[i],b=p[i],c=p[i+1],e=p[i+2]||c;d+=` C ${b[0]+(c[0]-a[0])/6} ${b[1]+(c[1]-a[1])/6} ${c[0]-(e[0]-b[0])/6} ${c[1]-(e[1]-b[1])/6} ${c[0]} ${c[1]}`;}return d;};
const poly=(p)=>p.map((q,i)=>(i?'L':'M')+q[0]+' '+q[1]).join(' ');
const area=(p,base)=>`M ${p[0][0]} ${base} `+p.map(q=>`L ${q[0]} ${q[1]}`).join(' ')+` L ${p[p.length-1][0]} ${base} Z`;

const Reveal=({children,delay=0,y=24,className=''})=>(
  <motion.div className={className} initial={{opacity:0,y}} whileInView={{opacity:1,y:0}}
    viewport={{once:true,amount:0.35}} transition={{duration:0.8,delay,ease:EASE}}>{children}</motion.div>
);

const SectionLabel=({idx,zh})=>(
  <Reveal>
    <div className="flex items-center gap-3">
      <span style={{fontFamily:MONO,color:GREEN}} className="text-sm font-medium">{idx}</span>
      <span className="h-px w-10" style={{background:`linear-gradient(90deg,${GREEN},transparent)`}}/>
      <span style={{fontFamily:MONO,color:TXT}} className="text-base font-semibold tracking-widest">{zh}</span>
    </div>
  </Reveal>
);

const ChartFrame=({children})=>(
  <Reveal delay={0.12}>
    <figure className="mt-8 overflow-hidden rounded-2xl border p-4"
      style={{borderColor:HAIR,background:'linear-gradient(180deg,#0F151E,#0B1118)'}}>
      {children}
    </figure>
  </Reveal>
);

const MartingaleChart=()=>{
  const g=[[10,165],[40,150],[60,160],[90,134],[115,148],[145,118],[170,132],[200,96],[225,110],[255,72],[280,86],[312,46]];
  const r=[[10,160],[60,150],[110,140],[160,130],[210,120],[245,113]];
  return(
    <svg viewBox="0 0 320 200" className="w-full h-56">
      <defs>
        <linearGradient id="marting" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={GREEN} stopOpacity="0.25"/><stop offset="100%" stopColor={GREEN} stopOpacity="0"/></linearGradient>
      </defs>
      <line x1="6" y1="175" x2="314" y2="175" stroke={HAIR}/>
      <motion.path d={poly(r)+' L 245 184 L 280 184'} fill="none" stroke={RED} strokeWidth="2.2" strokeLinecap="round"
        initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.8,ease:'easeInOut'}}/>
      <motion.circle cx="263" cy="184" r="4" fill={RED} initial={{opacity:0,scale:0}} whileInView={{opacity:1,scale:1}} viewport={{once:true,amount:0.5}} transition={{delay:1.8}}/>
      <motion.circle cx="263" cy="184" r="4" fill="none" stroke={RED} strokeWidth="1.5"
        initial={{opacity:0}} whileInView={{opacity:[0.8,0],scale:[1,3]}} viewport={{once:true,amount:0.5}} transition={{delay:1.9,duration:1.4,repeat:Infinity}}/>
      <motion.text x="280" y="171" textAnchor="end" style={{fontFamily:MONO}} fontSize="11" fill={RED}
        initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{delay:2}}>−100% 归零</motion.text>
      <motion.text x="58" y="136" style={{fontFamily:MONO}} fontSize="11" fill={RED}
        initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{delay:0.8}}>马丁策略</motion.text>
      <motion.path d={area(g,175)} fill="url(#marting)" initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.8,delay:0.4}}/>
      <motion.path d={poly(g)} fill="none" stroke={GREEN} strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round"
        initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.8,ease:'easeInOut',delay:0.4}}/>
      <motion.text x="300" y="38" textAnchor="end" style={{fontFamily:MONO}} fontSize="11" fill={GREEN}
        initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{delay:2.2}}>本策略 · 阶梯向上</motion.text>
    </svg>
  );
};

const RecoveryChart = () => {
  const cx = 95;
  const rows = [
    { label: '日常波动', down: 10, up: 11.1, downW: 10, upW: 11, color: DIM },
    { label: '本策略极限', down: 20.5, up: 25.8, downW: 20.5, upW: 25.8, color: GREEN, focus: true },
    { label: '腰斩警戒线', down: 50, up: 100, downW: 50, upW: 100, color: GOLD },
    { label: '万劫不复', down: 90, up: 900, downW: 90, upW: 170, color: RED, isMax: true }
  ];

  return (
    <svg viewBox="0 0 320 230" className="w-full h-64">
      <defs>
        <linearGradient id="fadeRed" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor={RED} stopOpacity="1"/>
          <stop offset="60%" stopColor={RED} stopOpacity="0.8"/>
          <stop offset="100%" stopColor={INK} stopOpacity="0"/>
        </linearGradient>
        <linearGradient id="focusGreen" x1="0" y1="0" x2="1" y2="0">
          <stop offset="0%" stopColor={GREEN} stopOpacity="0.15"/>
          <stop offset="100%" stopColor={GREEN} stopOpacity="0"/>
        </linearGradient>
      </defs>

      <text x={cx - 10} y="20" style={{fontFamily:MONO}} fontSize="11" fill={DIM} textAnchor="end">跌幅 ↓</text>
      <line x1={cx} y1="12" x2={cx} y2="24" stroke={HAIR} />
      <text x={cx + 10} y="20" style={{fontFamily:MONO}} fontSize="11" fill={DIM} textAnchor="start">回本需涨幅 ↑</text>

      <motion.line x1={cx} y1="35" x2={cx} y2="225" stroke={HAIR} strokeDasharray="2 3"
        initial={{pathLength:0}} whileInView={{pathLength:1}} transition={{duration:1}} />

      {rows.map((r, i) => {
        const y = 55 + i * 48;
        return (
          <g key={i}>
            {r.focus && (
              <motion.rect x="0" y={y-22} width="320" height="42" fill="url(#focusGreen)"
                initial={{opacity:0}} whileInView={{opacity:1}} transition={{delay:0.5}} />
            )}
            <text x={cx + 10} y={y - 12} style={{fontFamily:SANS}} fontSize="12" fill={r.focus ? GREEN : TXT} opacity={r.focus ? 1 : 0.6}>
              {r.label}
            </text>
            <motion.rect
              x={cx - r.downW} y={y - 5} width={r.downW} height="10" rx="3"
              fill={r.focus ? 'rgba(52,224,161,0.4)' : 'rgba(239,91,91,0.4)'}
              style={{ transformOrigin: 'right' }}
              initial={{ scaleX: 0 }} whileInView={{ scaleX: 1 }}
              viewport={{ once: true, amount: 0.5 }}
              transition={{ duration: 0.8, delay: i * 0.15 + 0.2, ease: 'easeOut' }}
            />
            <motion.text x={cx - r.downW - 8} y={y + 4} style={{fontFamily:MONO}} fontSize="11" fill={r.focus ? GREEN : RED} textAnchor="end"
              initial={{ opacity: 0, x: 5 }} whileInView={{ opacity: 1, x: 0 }} transition={{ delay: i * 0.15 + 0.6 }}>
              −{r.down}%
            </motion.text>
            <motion.rect
              x={cx} y={y - 5} width={r.upW} height="10" rx="3"
              fill={r.focus ? GREEN : (r.isMax ? 'url(#fadeRed)' : TXT)}
              style={{ transformOrigin: 'left' }}
              initial={{ scaleX: 0 }} whileInView={{ scaleX: 1 }}
              viewport={{ once: true, amount: 0.5 }}
              transition={{ duration: 1.2, delay: i * 0.15 + 0.3, type: 'spring', bounce: 0.25 }}
            />
            <motion.text x={cx + r.upW + (r.isMax ? 0 : 8)} y={y + 4} style={{fontFamily:MONO}} fontSize="11" fill={r.focus ? GREEN : (r.isMax ? RED : TXT)} textAnchor="start"
              initial={{ opacity: 0, x: -5 }} whileInView={{ opacity: 1, x: 0 }} transition={{ delay: i * 0.15 + 0.8 }}>
              +{r.up}% {r.isMax && <tspan fill={RED} fontSize="14" dy="2">∞</tspan>}
            </motion.text>
          </g>
        )
      })}
    </svg>
  );
};

const BalanceScale=()=>{
  const loss=[[50,108],[62,108],[74,108],[55,98],[67,98],[61,89],[44,116],[80,116]];
  const gain=[[236,104],[262,104],[249,82]];
  return(
    <svg viewBox="0 0 320 200" className="w-full h-56">
      <defs>
        <radialGradient id="redCoin" cx="35%" cy="35%" r="65%"><stop offset="0%" stopColor="#ff8a8a"/><stop offset="100%" stopColor={RED}/></radialGradient>
        <radialGradient id="greenCoin" cx="35%" cy="35%" r="65%"><stop offset="0%" stopColor="#7affca"/><stop offset="100%" stopColor={GREEN}/></radialGradient>
      </defs>
      <line x1="40" y1="180" x2="280" y2="180" stroke={HAIR}/>
      <path d="M160 118 L150 180 L170 180 Z" fill="rgba(255,255,255,0.12)" stroke="rgba(255,255,255,0.25)"/>
      <motion.g style={{transformBox:'view-box',transformOrigin:'160px 118px'}}
        initial={{rotate:0}} whileInView={{rotate:7}} viewport={{once:true,amount:0.5}} transition={{delay:1,duration:1.2,ease:EASE}}>
        <rect x="40" y="116" width="240" height="5" rx="2.5" fill={TXT} opacity="0.85"/>
        <circle cx="160" cy="118" r="6" fill={INK} stroke={TXT}/>
        {loss.map((p,i)=>(<motion.circle key={'l'+i} cx={p[0]} cy={p[1]} r="6" fill="url(#redCoin)"
          initial={{opacity:0,cy:p[1]-30}} whileInView={{opacity:0.9,cy:p[1]}} viewport={{once:true,amount:0.5}}
          transition={{delay:0.1+i*0.06,type:'spring',stiffness:200,damping:14}}/>))}
        {gain.map((p,i)=>(<motion.circle key={'g'+i} cx={p[0]} cy={p[1]} r="13" fill="url(#greenCoin)"
          initial={{opacity:0,cy:p[1]-30}} whileInView={{opacity:0.95,cy:p[1]}} viewport={{once:true,amount:0.5}}
          transition={{delay:0.3+i*0.12,type:'spring',stiffness:200,damping:14}}/>))}
      </motion.g>
      <text x="62" y="160" textAnchor="middle" style={{fontFamily:MONO}} fontSize="11" fill={RED}>亏损 · 多而小</text>
      <text x="250" y="160" textAnchor="middle" style={{fontFamily:MONO}} fontSize="11" fill={GREEN}>盈利 · 少而大</text>
    </svg>
  );
};

const BearChart=()=>{
  const bench=[[10,45],[55,55],[105,72],[155,95],[205,114],[255,126],[290,131]];
  const strat=[[10,45],[55,42],[105,46],[155,40],[205,44],[255,37],[290,34]];
  return(
    <svg viewBox="0 0 300 150" className="w-full h-40">
      <defs>
        <linearGradient id="bearg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={GREEN} stopOpacity="0.25"/><stop offset="100%" stopColor={GREEN} stopOpacity="0"/></linearGradient>
      </defs>
      <line x1="6" y1="45" x2="294" y2="45" stroke={HAIR} strokeDasharray="3 3"/>
      <motion.path d={area(strat, 140)} fill="url(#bearg)" initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{duration:1,delay:0.3}}/>
      <motion.path d={smooth(bench)} fill="none" stroke={RED} strokeWidth="2" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.4,ease:'easeInOut'}}/>
      <motion.path d={smooth(strat)} fill="none" stroke={GREEN} strokeWidth="2.2" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.4,ease:'easeInOut',delay:0.3}}/>
      <text x="8" y="40" style={{fontFamily:MONO}} fontSize="11" fill={DIM}>0</text>
    </svg>
  );
};

const BullChart=()=>{
  const strat=[[10,128],[50,118],[90,104],[130,86],[170,64],[210,44],[250,28],[290,16]];
  const bench=[[10,128],[55,118],[105,108],[155,98],[205,90],[255,84],[290,80]];
  return(
    <svg viewBox="0 0 300 150" className="w-full h-40">
      <defs><linearGradient id="bullg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={GREEN} stopOpacity="0.28"/><stop offset="100%" stopColor={GREEN} stopOpacity="0"/></linearGradient></defs>
      <motion.path d={area(strat,140)} fill="url(#bullg)" initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{duration:1,delay:0.6}}/>
      <motion.path d={smooth(bench)} fill="none" stroke={DIM} strokeWidth="1.6" strokeDasharray="4 4" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.4}}/>
      <motion.path d={smooth(strat)} fill="none" stroke={GREEN} strokeWidth="2.4" strokeLinecap="round" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.6,ease:'easeInOut',delay:0.3}}/>
    </svg>
  );
};

const BullBearTabs = () => {
  const [tab, setTab] = useState('bear');

  useEffect(() => {
    const timer = setInterval(() => {
      setTab(prev => prev === 'bear' ? 'bull' : 'bear');
    }, 3000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="flex flex-col">
      <div className="mb-6 flex w-full rounded-lg bg-[#0F151E] p-1 border border-white/5">
        <button onClick={()=>setTab('bear')} className={`flex-1 rounded-md py-2 text-xs font-bold tracking-widest transition-colors ${tab==='bear'?'bg-[#34E0A1]/15 text-[#34E0A1]':'text-[#8A93A3]'}`}>熊市不亏</button>
        <button onClick={()=>setTab('bull')} className={`flex-1 rounded-md py-2 text-xs font-bold tracking-widest transition-colors ${tab==='bull'?'bg-[#34E0A1]/15 text-[#34E0A1]':'text-[#8A93A3]'}`}>牛市起飞</button>
      </div>
      <div className="relative min-h-[190px]">
        <AnimatePresence mode="wait">
          {tab === 'bear' ? (
            <motion.div key="bear" initial={{opacity:0,y:10}} animate={{opacity:1,y:0}} exit={{opacity:0,y:-10}} transition={{duration:0.3}}>
              <div className="mb-2 flex items-center justify-end">
                <span style={{fontFamily:MONO,color:DIM}} className="text-xs">基准 <span style={{color:RED}}>−38.2%</span> · 策略 <span style={{color:GREEN}}>+6.5%</span></span>
              </div>
              <BearChart/>
              <p style={{fontFamily:MONO,color:GREEN}} className="mt-2 text-xs">↑ 超额收益 +44.7%</p>
            </motion.div>
          ) : (
            <motion.div key="bull" initial={{opacity:0,y:10}} animate={{opacity:1,y:0}} exit={{opacity:0,y:-10}} transition={{duration:0.3}}>
              <div className="mb-2 flex items-center justify-end">
                <span style={{fontFamily:MONO,color:DIM}} className="text-xs">基准 +120% · 策略 <span style={{color:GREEN}}>+312%</span></span>
              </div>
              <BullChart/>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
};

const SystematicChart=()=>{
  const line=[[10,120],[45,112],[80,101],[115,92],[150,79],[185,68],[220,53],[255,40],[290,28]];
  return(
    <svg viewBox="0 0 300 150" className="w-full h-44">
      <defs><linearGradient id="sysg" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor={GREEN} stopOpacity="0.22"/><stop offset="100%" stopColor={GREEN} stopOpacity="0"/></linearGradient></defs>
      {[30,60,90,120].map(y=><line key={y} x1="6" y1={y} x2="294" y2={y} stroke={HAIR}/>)}
      {[50,100,150,200,250].map(x=><line key={x} x1={x} y1="10" x2={x} y2="135" stroke={HAIR}/>)}
      <motion.rect x="0" y="10" width="2" height="125" fill={GREEN} opacity="0.25" initial={{x:8}} animate={{x:[8,288]}} transition={{duration:4,repeat:Infinity,ease:'linear'}}/>
      <motion.path d={area(line,135)} fill="url(#sysg)" initial={{opacity:0}} whileInView={{opacity:1}} viewport={{once:true,amount:0.5}} transition={{duration:1,delay:0.8}}/>
      <motion.path d={smooth(line)} fill="none" stroke={GREEN} strokeWidth="2.4" strokeLinecap="round" initial={{pathLength:0}} whileInView={{pathLength:1}} viewport={{once:true,amount:0.5}} transition={{duration:1.8,ease:'easeInOut'}}/>
      {line.map((p,i)=>(<motion.circle key={i} cx={p[0]} cy={p[1]} r="2.6" fill={INK} stroke={GREEN} strokeWidth="1.5"
        initial={{opacity:0,scale:0}} whileInView={{opacity:1,scale:1}} viewport={{once:true,amount:0.5}} transition={{delay:0.5+i*0.13}}/>))}
    </svg>
  );
};

const RuleStatus=()=>{
  const rules=['RULE_01  趋势确认','RULE_02  仓位控制','RULE_03  止损纪律','RULE_04  系统执行'];
  return(
    <div className="mt-4 flex flex-col gap-2">
      {rules.map((r,i)=>(
        <Reveal key={i} delay={0.5+i*0.15}>
          <div className="flex items-center justify-between rounded-lg px-3 py-2" style={{background:'rgba(255,255,255,0.03)'}}>
            <span style={{fontFamily:MONO,color:DIM}} className="text-xs tracking-wider">{r}</span>
            <span style={{fontFamily:MONO,color:GREEN}} className="flex items-center gap-1 text-xs"><Check size={12} strokeWidth={3}/> OK</span>
          </div>
        </Reveal>
      ))}
      <Reveal delay={1.2}>
        <div className="mt-1 flex items-center gap-2" style={{color:GREEN,fontFamily:MONO}}>
          <motion.span animate={{opacity:[1,0.3,1]}} transition={{repeat:Infinity,duration:1.6}} className="inline-block h-1.5 w-1.5 rounded-full" style={{background:GREEN}}/>
          <span className="text-xs tracking-widest">SYSTEM · ACTIVE · 安静运行</span>
        </div>
      </Reveal>
    </div>
  );
};

const ScrollBar=({ scrollRef })=>{
  const {scrollYProgress}=useScroll({ container: scrollRef });
  return <motion.div className="fixed left-0 top-0 z-50 h-0.5 w-full" style={{scaleX:scrollYProgress,transformOrigin:'0%',background:GREEN}}/>;
};

const Hero=()=>(
  <section className="relative flex min-h-screen flex-col justify-center py-24 mx-auto w-full max-w-md px-6" style={{ scrollSnapAlign: 'center', scrollSnapStop: 'always' }}>
    <svg viewBox="0 0 320 200" preserveAspectRatio="xMidYMid slice" className="pointer-events-none absolute inset-x-0 bottom-10 w-full opacity-10">
      <path d={poly([[0,180],[60,170],[60,150],[120,140],[120,120],[190,110],[190,86],[255,74],[255,50],[320,36]])} fill="none" stroke={GREEN} strokeWidth="1.5"/>
    </svg>
    <motion.h2 initial={{opacity:0,y:16}} animate={{opacity:1,y:0}} transition={{duration:0.8,ease:EASE}}
      style={{fontFamily:SERIF,color:TXT}} className="mb-8 text-3xl font-medium tracking-wide">
      怎样才能<span style={{color:GOLD, textShadow:'0 0 20px rgba(231,200,132,0.4)'}}>赚钱</span>？
    </motion.h2>
    <motion.h1 initial={{opacity:0,y:20}} animate={{opacity:1,y:0}} transition={{duration:0.9,delay:0.15,ease:EASE}}
      style={{fontFamily:SERIF,color:TXT}} className="text-5xl font-semibold leading-tight tracking-wide">
      不求常胜，<br/>但求<span style={{color:GOLD,textShadow:'0 0 30px rgba(231,200,132,0.35)'}}>大胜</span>。
    </motion.h1>
    <motion.p initial={{opacity:0,y:20}} animate={{opacity:1,y:0}} transition={{duration:0.9,delay:0.5,ease:EASE}}
      style={{color:DIM}} className="mt-8 text-lg leading-relaxed">
      穿越牛熊的，不是<span style={{color:TXT}}>预测</span>，是<span style={{color:GREEN}}>结构</span>。
    </motion.p>
    <motion.div initial={{opacity:0}} animate={{opacity:1}} transition={{delay:1.2,duration:1}}
      className="absolute inset-x-0 bottom-8 flex flex-col items-center gap-1" style={{color:DIM}}>
      <motion.div animate={{y:[0,6,0]}} transition={{repeat:Infinity,duration:1.8}}><ChevronsDown size={18}/></motion.div>
      <span style={{fontFamily:MONO}} className="text-xs tracking-widest">SCROLL</span>
    </motion.div>
  </section>
);

const Principle=({idx,zh,maxim,takeaway,children})=>(
  <section className="flex min-h-screen flex-col justify-center py-24 mx-auto w-full max-w-md px-6" style={{ scrollSnapAlign: 'center', scrollSnapStop: 'always' }}>
    <SectionLabel idx={idx} zh={zh}/>
    <Reveal delay={0.05}>
      <h2 style={{fontFamily:SERIF,color:TXT}} className="mt-7 text-3xl font-semibold leading-snug tracking-wide">{maxim}</h2>
    </Reveal>
    <ChartFrame>{children}</ChartFrame>
    <Reveal delay={0.1}>
      <div className="mt-7 inline-flex items-center gap-3 border-l-[3px] py-2.5 pl-4 pr-6"
        style={{borderColor: GREEN, background: 'linear-gradient(90deg, rgba(52,224,161,0.1) 0%, transparent 100%)'}}>
        <span style={{fontFamily:MONO,color:GREEN}} className="text-xs font-bold tracking-wider">本策略</span>
        <span className="h-3 w-px" style={{background:'rgba(52,224,161,0.3)'}}/>
        <span style={{color:TXT}} className="text-base font-bold tracking-wide">{takeaway}</span>
      </div>
    </Reveal>
  </section>
);

const WeChatIcon = ({ size = 24, className = "" }) => (
  <svg viewBox="0 0 1024 1024" width={size} height={size} className={className} fill="currentColor">
    <path d="M682.667 768c-23.467 0-46.934-4.267-66.134-10.667L541.867 800c-12.8 6.4-27.734 0-32-10.667-2.134-8.533-2.134-14.933 0-21.333l19.2-57.6c-49.067-34.133-78.934-83.2-78.934-138.667 0-100.266 98.134-181.333 219.734-181.333 119.466 0 219.733 81.067 219.733 181.333s-98.133 181.333-219.733 181.334z m130.133-270.933c10.667 0 19.2-8.534 19.2-19.2s-8.533-19.2-19.2-19.2-19.2 8.533-19.2 19.2 8.533 19.2 19.2 19.2z m-162.133-38.4c-10.667 0-19.2 8.533-19.2 19.2s8.533 19.2 19.2 19.2 19.2-8.533 19.2-19.2-8.533-19.2-19.2-19.2zM401.067 661.333c-36.267 0-70.4-10.667-100.267-25.6l-98.133 53.334c-17.067 8.533-36.267 0-42.667-14.934-2.133-10.666-2.133-19.2 0-29.866l23.467-78.934c-61.867-51.2-100.267-117.333-100.267-194.133 0-145.067 140.8-262.4 313.6-262.4 174.933 0 315.733 117.333 315.733 262.4 0 27.733-4.267 55.467-12.8 81.067-10.667-2.133-21.333-2.133-32-2.133-142.933 0-258.133 100.267-258.133 226.133 0 59.733 27.733 115.2 76.8 153.6-27.733 21.333-57.6 32-85.333 32z m-123.734-352c-14.933 0-27.733 12.8-27.733 27.734s12.8 27.733 27.733 27.733 27.733-12.8 27.733-27.733-12.8-27.734-27.733-27.734z m219.734 0c-14.934 0-27.734 12.8-27.734 27.734s12.8 27.733 27.734 27.733 27.733-12.8 27.733-27.733-12.8-27.734-27.733-27.734z"/>
  </svg>
);

const Finale=({ onViewRadar })=>{
  const HOLD=2400;
  const feats=['不预测，只跟随趋势','先求不死，再求大胜','亏有底线，赢无上限','熊市不亏，牛市起飞','规则驱动，透明可验'];
  const TH=[0.12,0.30,0.48,0.66,0.84];

  const [progress,setProgress]=useState(0),
        [holding,setHolding]=useState(false),
        [done,setDone]=useState(false),
        [val,setVal]=useState(0);

  const pRef=useRef(0), raf=useRef(0), last=useRef(0);
  const progressSpanRef = useRef(null);
  const buttonRef = useRef(null);

  const loop = now => {
    const dt = now - last.current; last.current = now;
    let p = pRef.current + dt / HOLD;

    if (p >= 1) {
      pRef.current = 1;
      setProgress(1);
      if (progressSpanRef.current) progressSpanRef.current.style.width = '100%';
      if (buttonRef.current) buttonRef.current.style.boxShadow = `0 0 35px rgba(52,224,161,0.35)`;
      setHolding(false);
      setDone(true);
      return;
    }

    const oldRevealed = TH.filter(t => pRef.current >= t).length;
    const newRevealed = TH.filter(t => p >= t).length;
    if (newRevealed !== oldRevealed) setProgress(p);

    pRef.current = p;
    if (progressSpanRef.current) progressSpanRef.current.style.width = `${p * 100}%`;
    if (buttonRef.current) buttonRef.current.style.boxShadow = `0 0 ${15 + p * 20}px rgba(52,224,161,${0.15 + p * 0.2})`;

    raf.current = requestAnimationFrame(loop);
  };

  const start = (e) => {
    if(done || holding) return;
    if(e && e.preventDefault) e.preventDefault();
    last.current = performance.now();
    cancelAnimationFrame(raf.current);
    raf.current = requestAnimationFrame(loop);
    setHolding(true);
  };

  useEffect(()=>()=>cancelAnimationFrame(raf.current),[]);
  useEffect(()=>{
    if(!done) return;
    let r; const s=performance.now();
    const t=now=>{
      const k=Math.min(1,(now-s)/1500);
      setVal(1962.9*(1-Math.pow(1-k,3)));
      if(k<1) r=requestAnimationFrame(t);
    };
    r=requestAnimationFrame(t);
    return()=>cancelAnimationFrame(r);
  },[done]);

  const reset=()=>{
    setDone(false);
    setProgress(0);
    setVal(0);
    setHolding(false);
    pRef.current=0;
    if(progressSpanRef.current) progressSpanRef.current.style.width = '0%';
    if(buttonRef.current) buttonRef.current.style.boxShadow = `0 0 15px rgba(52,224,161,0.15)`;
  };

  const revealed=TH.filter(t=>progress>=t).length;
  const eq=[[8,152],[28,150],[48,146],[68,149],[90,140],[110,143],[132,132],[154,135],[176,120],[198,123],[220,104],[242,100],[262,78],[282,58],[300,38],[314,22]];

  return(
    <section className="relative flex min-h-screen flex-col justify-center py-24 mx-auto w-full max-w-md px-6" style={{ scrollSnapAlign: 'center', scrollSnapStop: 'always' }}>
      {done&&<div className="pointer-events-none fixed inset-0" style={{background:'radial-gradient(circle at 50% 40%, rgba(52,224,161,0.10), transparent 60%)'}}/>}
      <AnimatePresence mode="wait">
      {!done?(
          <motion.div key="pre" exit={{opacity: 0, scale: 0.96}} transition={{duration: 0.5}}
                      className="flex flex-col items-center text-center w-full">
              <Reveal><p style={{fontFamily: SERIF, color: TXT}} className="text-4xl font-semibold leading-snug">五条法则，<br/>一条曲线。
              </p></Reveal>
              <Reveal delay={0.1}><p style={{color: DIM}} className="mt-5 text-base leading-relaxed">按住下方，<br/>见证它穿越牛熊。
              </p></Reveal>
              <div className="mt-8 mb-8 flex w-full flex-col gap-2.5">
                  {feats.map((f, i) => (
                      <motion.div key={i} animate={{opacity: i < revealed ? 1 : 0.18}} transition={{duration: 0.4}}
                                  className="flex items-center gap-3 rounded-xl border px-4 py-2.5"
                                  style={{
                                      borderColor: i < revealed ? 'rgba(52,224,161,0.3)' : HAIR,
                                      background: i < revealed ? 'rgba(52,224,161,0.06)' : 'transparent'
                                  }}>
                          <motion.span animate={{scale: i < revealed ? 1 : 0.6}}
                                       className="flex h-5 w-5 items-center justify-center rounded-full"
                                       style={{
                                           background: i < revealed ? GREEN : 'transparent',
                                           border: i < revealed ? 'none' : `1px solid ${HAIR}`
                                       }}>
                              {i < revealed && <Check size={13} color={INK} strokeWidth={3}/>}
                          </motion.span>
                          <span style={{color: i < revealed ? TXT : DIM}} className="text-sm font-medium">{f}</span>
                      </motion.div>
                  ))}
              </div>

              <button onPointerDown={start} onClick={start}
                      onContextMenu={e => e.preventDefault()}
                      onTouchStart={e => { }}
                      ref={buttonRef}
                      style={{
                          touchAction: 'none',
                          userSelect: 'none',
                          WebkitUserSelect: 'none',
                          WebkitTouchCallout: 'none',
                          WebkitTapHighlightColor: 'transparent',
                          borderColor: GREEN,
                          transform: 'translateZ(0)',
                          boxShadow: `0 0 ${15 + pRef.current * 20}px rgba(52,224,161,${0.15 + pRef.current * 0.2})`
                      }}
                      className="relative w-full overflow-hidden rounded-full border-2 px-6 py-4 cursor-pointer">

                  <span ref={progressSpanRef} className="absolute inset-y-0 left-0" style={{
                      width: `${pRef.current * 100}%`,
                      background: 'rgba(52,224,161,0.22)'
                  }}/>

                  <span className="relative flex items-center justify-center gap-2"
                        style={{color: GREEN, transform: 'translateZ(0)'}}>
                    <Fingerprint size={18}/>
                    <span
                        className="text-base font-semibold tracking-wide">{holding ? '正在揭晓…' : '按住不放 · 见证穿越牛熊的威力'}</span>
                  </span>
              </button>
              <motion.p animate={{opacity: holding ? 0.4 : 0.7}} style={{fontFamily: MONO, color: DIM}}
                        className="mt-4 text-xs tracking-widest">
                  {holding ? 'LOADING ···' : 'PRESS & HOLD · 长按解锁'}</motion.p>
          </motion.div>
      ) : (
          <motion.div key="post" initial={{opacity: 0, scale: 0.96}} animate={{opacity: 1, scale: 1}}
                      transition={{duration: 0.6, ease: EASE}} className="w-full">
              <p style={{fontFamily: MONO, color: DIM}} className="text-xs tracking-widest uppercase">Cumulative Return · 累计收益率 · 回测</p>
              <div className="mt-2 flex items-end gap-1">
                  <span style={{fontFamily: MONO, color: GOLD}} className="text-2xl font-bold">+</span>
                  <span style={{fontFamily: MONO, color: GOLD, textShadow: '0 0 40px rgba(231,200,132,0.4)'}}
                        className="text-6xl font-bold tracking-tight tabular-nums">{val.toFixed(1)}</span>
                  <span style={{fontFamily: MONO, color: GOLD}} className="mb-2 text-3xl font-bold">%</span>
              </div>
              <div className="mt-6 rounded-2xl border p-4"
                   style={{borderColor: HAIR, background: 'linear-gradient(180deg,#0F151E,#0B1118)'}}>
                  <svg viewBox="0 0 320 170" className="w-full h-44">
                      <defs>
                          <linearGradient id="eqg" x1="0" y1="0" x2="0" y2="1">
                              <stop offset="0%" stopColor={GREEN} stopOpacity="0.3"/>
                              <stop offset="100%" stopColor={GREEN} stopOpacity="0"/>
                          </linearGradient>
                      </defs>
                      <motion.path d={area(eq, 160)} fill="url(#eqg)" initial={{opacity: 0}} animate={{opacity: 1}}
                                   transition={{duration: 2, delay: 0.4}}/>
                      <motion.path d={smooth(eq)} fill="none" stroke={GREEN} strokeWidth="2.6" strokeLinecap="round" initial={{pathLength:0}} animate={{pathLength:1}} transition={{duration:3,ease:'easeOut'}}/>
              <line x1="6" y1="160" x2="314" y2="160" stroke={HAIR}/>
            </svg>
          </div>

          <div className="mt-5 flex flex-col gap-3">
            <div className="rounded-xl border p-4 flex justify-between items-center relative overflow-hidden" style={{borderColor:HAIR, background:'linear-gradient(135deg, rgba(52,224,161,0.12) 0%, rgba(52,224,161,0.02) 100%)'}}>
              <div className="absolute top-0 right-0 rounded-bl-lg px-2 py-0.5" style={{background:'rgba(52,224,161,0.2)'}}>
                <span style={{fontSize:'9px', color:GREEN, fontWeight:'bold'}}>核心优势</span>
              </div>
              <div>
                <p style={{fontFamily:MONO,color:DIM}} className="text-xs tracking-wider">平均年化收益</p>
                <p style={{fontFamily:MONO,color:GREEN}} className="mt-1 text-2xl font-bold tabular-nums">76.7%</p>
              </div>
              <div className="h-10 w-px" style={{background:'rgba(52,224,161,0.2)'}}></div>
              <div className="text-right pr-2">
                <p style={{fontFamily:MONO,color:DIM}} className="text-xs tracking-wider">盈亏比</p>
                <p style={{fontFamily:MONO,color:GREEN}} className="mt-1 text-2xl font-bold tabular-nums">2.01</p>
              </div>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div className="rounded-xl border p-3.5" style={{borderColor:HAIR}}>
                <p style={{fontFamily:MONO,color:DIM}} className="text-[11px] tracking-wider mb-1">测试 5 年完成</p>
                <p style={{fontFamily:MONO,color:TXT}} className="text-lg font-bold tabular-nums">1280 <span className="text-xs font-normal text-[#8A93A3]">笔</span></p>
              </div>
              <div className="rounded-xl border p-3.5" style={{borderColor:HAIR}}>
                <p style={{fontFamily:MONO,color:DIM}} className="text-[11px] tracking-wider mb-1">平均持仓时间</p>
                <p style={{fontFamily:MONO,color:TXT}} className="text-lg font-bold tabular-nums">38.5 <span className="text-xs font-normal text-[#8A93A3]">h</span></p>
              </div>
              <div className="rounded-xl border p-3.5 relative overflow-hidden" style={{borderColor:HAIR}}>
                 <div className="absolute top-0 right-0 rounded-bl-lg px-2 py-0.5" style={{background:'rgba(231,200,132,0.15)'}}>
                  <span style={{fontSize:'9px', color:GOLD}}>真实特征</span>
                </div>
                <p style={{fontFamily:MONO,color:DIM}} className="text-[11px] tracking-wider mb-1">策略胜率</p>
                <p style={{fontFamily:MONO,color:GOLD}} className="text-lg font-bold tabular-nums">41.9%</p>
              </div>
              <div className="rounded-xl border p-3.5 relative overflow-hidden" style={{borderColor:HAIR}}>
                 <div className="absolute top-0 right-0 rounded-bl-lg px-2 py-0.5" style={{background:'rgba(239,91,91,0.15)'}}>
                  <span style={{fontSize:'9px', color:RED}}>极限风险</span>
                </div>
                <p style={{fontFamily:MONO,color:DIM}} className="text-[11px] tracking-wider mb-1">最大回撤</p>
                <p style={{fontFamily:MONO,color:RED}} className="text-lg font-bold tabular-nums">−20.5%</p>
              </div>
            </div>
          </div>

          <motion.div initial={{opacity: 0, y: 15}} animate={{opacity: 1, y: 0}} transition={{delay: 2.8, duration: 0.8, ease: EASE}}>
            <button
              onClick={onViewRadar}
              style={{
                borderColor: GREEN,
                background: 'rgba(52,224,161,0.08)'
              }}
              className="mt-8 relative flex w-full flex-col items-center justify-center gap-1.5 rounded-xl border py-4 transition-all duration-500 shadow-[0_0_20px_rgba(52,224,161,0.12)] active:scale-[0.98]"
            >
              <div style={{color: GREEN}} className="flex items-center gap-2.5 text-[16px] font-bold tracking-widest">
                <Activity size={18} />
                <span>查看实时交易信号</span>
              </div>
            </button>

            <button onClick={reset} style={{borderColor:HAIR,color:DIM,fontFamily:MONO}} className="mt-4 mx-auto block rounded-full border px-4 py-2 text-xs tracking-widest transition-colors hover:text-white">↻ 重新演示</button>

            <p style={{color:DIM}} className="mt-8 text-center text-xs leading-relaxed opacity-60">*历史回测数据，不代表未来收益，不构成投资建议。</p>
          </motion.div>
        </motion.div>
      )}
      </AnimatePresence>
    </section>
  );
};

const SignalRadar = ({ onBack }) => {
  const WECHAT_ID = 'Alpha_Quant_01';
  const [agreed, setAgreed] = useState(false);
  const [showModal, setShowModal] = useState(false);

  // --- API 数据状态 ---
  const [signalData, setSignalData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [fetchTime, setFetchTime] = useState('--');

  const fetchSignals = async () => {
    setIsLoading(true);
    try {
      // 请将此 URL 替换为你 Flask API 实际部署的地址
      const response = await fetch(`http://127.0.0.1:5000/api/signals?t=${Date.now()}`);
      if (!response.ok) throw new Error('网络请求失败');

      const data = await response.json();
      setSignalData(data);

      // 前端发起请求的本地时刻
      const now = new Date();
      const pad = (n) => n.toString().padStart(2, '0');
      setFetchTime(`${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())} ${pad(now.getHours())}:${pad(now.getMinutes())}:${pad(now.getSeconds())}`);

    } catch (error) {
      console.error("获取信号失败:", error);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchSignals();
  }, []);

  const handleApply = () => {
    if (!agreed) return;
    try { navigator.clipboard.writeText(WECHAT_ID); } catch (err) {}
    setShowModal(true);
  };

  if (!signalData) {
    return (
      <div className="min-h-screen flex flex-col items-center justify-center w-full max-w-md mx-auto">
        <RefreshCw className="animate-spin text-[#3C82F6] mb-4" size={32} />
        <p className="text-[#8A93A3] text-sm tracking-widest" style={{fontFamily: MONO}}>正在获取最新雷达信号...</p>
      </div>
    );
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: 20 }}
      className="min-h-screen py-10 px-4 mx-auto w-full max-w-md"
    >
      <button onClick={onBack} className="flex items-center gap-1 text-[#8A93A3] text-sm mb-6 hover:text-white transition-colors">
        <ArrowLeft size={16}/> 返回回测
      </button>

      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-1.5 text-[#3C82F6]">
          <Zap size={20} fill="currentColor" />
          <span className="font-bold text-lg tracking-wide text-white">交易信号雷达</span>
        </div>
        <div className="flex items-center gap-2 text-[#8A93A3] text-xs">
          <div className="flex flex-col text-right">
            <span>最后更新</span>
            <span style={{fontFamily: MONO}}>{fetchTime}</span>
          </div>
          <button
            onClick={fetchSignals}
            disabled={isLoading}
            className={`p-1.5 rounded bg-[#3C82F6]/20 text-[#3C82F6] hover:bg-[#3C82F6]/30 transition-colors ${isLoading ? 'opacity-50 cursor-not-allowed' : ''}`}
          >
            <RefreshCw size={14} className={isLoading ? 'animate-spin' : ''} />
          </button>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-3 mb-8">
        <div className="bg-[#0F151E] border border-white/5 rounded-xl p-3 shadow-sm">
          <div className="flex items-center justify-between mb-2">
            <div className="flex items-center gap-1 text-[#E7C884] text-xs"><Database size={12}/>累计收益</div>
          </div>
          <div className="text-[#34E0A1] text-xl font-bold" style={{fontFamily: MONO}}>{signalData.stats.totalReturn}</div>
        </div>
        <div className="bg-[#0F151E] border border-white/5 rounded-xl p-3 shadow-sm">
          <div className="flex items-center gap-1 text-[#8A93A3] text-[10px] mb-1.5"><Clock size={10}/>统计信号时间范围</div>
          <div className="text-[#BCC2CE] text-[10px] leading-tight whitespace-pre-line" style={{fontFamily: MONO}}>
            {signalData.stats.timeRange}
          </div>
        </div>
      </div>

      <h3 className="text-white font-bold mb-4">当前持仓</h3>
      <div className="flex flex-col gap-4 mb-10">
        {signalData.currentPositions.length === 0 ? (
           <div className="text-center py-6 text-[#8A93A3] text-sm border border-dashed border-white/10 rounded-lg">暂无当前持仓</div>
        ) : (
          signalData.currentPositions.map((pos, idx) => {
            // 前端只负责拿后端的 isBuy 布尔值来分配颜色
            const themeColor = pos.isBuy ? '#34E0A1' : '#EF5B5B';

            return (
              <div key={idx} className="bg-[#0F151E] border border-y-white/5 border-r-white/5 rounded-lg p-4 shadow-sm" style={{ borderLeft: `3px solid ${themeColor}` }}>
                <div className="text-[#8A93A3] text-[10px] mb-3" style={{fontFamily: MONO}}>开仓时间 · {pos.time}</div>
                <div className="flex items-center gap-2 mb-4">
                  <span className="text-white text-2xl font-bold tracking-wider">{pos.symbol}</span>
                  <span className="text-white text-[10px] font-bold px-1.5 py-0.5 rounded" style={{ backgroundColor: themeColor }}>{pos.side}</span>
                </div>
                <div className="flex bg-[#161C24] rounded-lg p-3">
                  <div className="flex-1">
                    <div className="text-[#8A93A3] text-[10px] mb-1">开仓价格 (USDT)</div>
                    <div className="text-white font-bold" style={{fontFamily: MONO}}>{pos.price}</div>
                  </div>
                  <div className="flex-1">
                    <div className="text-[#8A93A3] text-[10px] mb-1">仓位占比</div>
                    <div className="text-[#3C82F6] font-bold" style={{fontFamily: MONO}}>{pos.size}</div>
                  </div>
                </div>
              </div>
            )
          })
        )}
      </div>

      <h3 className="text-[#8A93A3] font-bold mb-4">历史记录 (已平仓)</h3>
      <div className="flex flex-col gap-3 mb-12">
        {signalData.historyPositions.length === 0 ? (
           <div className="text-center py-6 text-[#8A93A3] text-sm border border-dashed border-white/10 rounded-lg">暂无历史记录</div>
        ) : (
          signalData.historyPositions.map((pos, idx) => {
            // 根据后端的 isBuyAction 判断红绿色系
            const actionColor = pos.isBuyAction ? '#34E0A1' : '#EF5B5B';
            const actionBorder = pos.isBuyAction ? 'rgba(52,224,161,0.3)' : 'rgba(239,91,91,0.3)';
            const actionBg = pos.isBuyAction ? 'rgba(52,224,161,0.1)' : 'rgba(239,91,91,0.1)';

            return (
              <div key={idx} className="bg-[#0F151E] border border-white/5 rounded-lg p-4 shadow-sm">
                <div className="flex justify-between items-center mb-3">
                  <div className="flex items-center gap-2">
                    <span className="text-white font-bold">{pos.symbol}</span>
                    <span className="border text-[10px] px-1.5 py-0.5 rounded" style={{ borderColor: actionBorder, color: actionColor, backgroundColor: actionBg }}>{pos.action}</span>
                  </div>
                  <span className={`font-bold ${pos.isWin ? 'text-[#34E0A1]' : 'text-[#EF5B5B]'}`} style={{fontFamily: MONO}}>{pos.pnl}</span>
                </div>
                <div className="h-px bg-white/5 mb-3 w-full" />
                <div className="flex justify-between items-end">
                  <div>
                    <div className="text-[#8A93A3] text-[10px] mb-1">开仓 · {pos.openTime}</div>
                    <div className="text-[#BCC2CE] text-xs" style={{fontFamily: MONO}}>{pos.openPrice}</div>
                  </div>
                  <div className="w-px h-6 bg-white/5 mx-2" />
                  <div className="text-right">
                    <div className="text-[#8A93A3] text-[10px] mb-1">平仓 · {pos.closeTime}</div>
                    <div className="text-[#BCC2CE] text-xs" style={{fontFamily: MONO}}>{pos.closePrice}</div>
                  </div>
                </div>
              </div>
            )
          })
        )}
      </div>

      <div className="mt-8 rounded-xl border border-dashed border-[#E7C884]/30 bg-[#E7C884]/[0.03] p-5">
        <div className="mb-4 flex items-center gap-2">
          <ShieldAlert size={16} color={GOLD}/>
          <span style={{fontFamily:MONO, color:GOLD}} className="text-xs font-bold tracking-widest">SYSTEM WARNING · 接入前必读</span>
        </div>
        <ul className="flex flex-col gap-3.5">
          {[
            {t:'极其枯燥乏味', d:'严格的信号过滤会导致极长时间空仓等待。如果您追求高频交易的刺激，请立即关闭本页面。'},
            {t:'存在连续止损', d:'胜率仅 41.9%，利润全靠高盈亏比。震荡市必定会经历连续的小额止损试错，以此换取大趋势的暴利。'},
            {t:'反人性执行', d:'放弃“一夜暴富”的短期幻想。本系统专注长线复利，市场狂热时可能强制空仓，需绝对服从机器纪律。'},
            {t:'拒绝造神神话', d:'绝不盲目抄底逃顶。系统严格执行右侧交易确认，主动放弃头部与尾部的高风险利润，只吃最确定的鱼身。'}
          ].map((item, i) => (
            <li key={i} className="flex flex-col">
              <span style={{color:TXT}} className="mb-1 text-sm font-semibold">· {item.t}</span>
              <span style={{color:DIM}} className="text-[11px] leading-relaxed">{item.d}</span>
            </li>
          ))}
        </ul>
      </div>

      <div
        className="mt-6 mb-6 flex cursor-pointer items-start gap-3 rounded-lg p-2 transition-colors hover:bg-white/[0.02]"
        onClick={()=>setAgreed(!agreed)}
      >
        <div className={`mt-0.5 flex h-4 w-4 shrink-0 items-center justify-center rounded border transition-all ${agreed ? 'border-[#34E0A1] bg-[#34E0A1]/20' : 'border-[#8A93A3]/50'}`}>
          {agreed && <Check size={12} color={GREEN} strokeWidth={4}/>}
        </div>
        <p style={{color: DIM}} className="text-[11px] leading-relaxed">
          我已理解：真正的交易是一场反人性的修行。<br/>想感受时间复利的魅力。
        </p>
      </div>

      <button
        onClick={agreed ? handleApply : undefined}
        disabled={!agreed}
        style={{
          borderColor: agreed ? GREEN : HAIR,
          background: agreed ? 'rgba(52,224,161,0.08)' : 'transparent',
          opacity: agreed ? 1 : 0.4,
          cursor: agreed ? 'pointer' : 'not-allowed'
        }}
        className={`relative flex w-full flex-col items-center justify-center gap-1.5 rounded-xl border py-4 mb-10 transition-all duration-500 ${agreed ? 'shadow-[0_0_20px_rgba(52,224,161,0.12)] active:scale-[0.98]' : ''}`}
      >
        <div style={{color: agreed ? GREEN : DIM}} className="flex items-center gap-2.5 text-[16px] font-bold tracking-widest transition-colors">
          {agreed ? <WeChatIcon size={18} /> : <Lock size={18} />}
          <span>{agreed ? '获取 Alpha 节点密钥' : '请先确认接受上述规则'}</span>
        </div>
        <span style={{fontFamily: MONO, color: DIM}} className="text-[11px] font-medium uppercase tracking-widest">
          Invitation Only · 凭口令获取信号
        </span>
      </button>

      <AnimatePresence>
        {showModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center p-6 backdrop-blur-md"
            style={{ background: 'rgba(10, 14, 20, 0.85)' }}
          >
            <motion.div
              initial={{ scale: 0.95, y: 10 }}
              animate={{ scale: 1, y: 0 }}
              exit={{ scale: 0.95, y: 10 }}
              className="relative w-full max-w-sm overflow-hidden rounded-2xl border border-white/10 bg-[#0A0E14] shadow-2xl"
            >
              <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-transparent via-[#34E0A1] to-transparent opacity-50"></div>

              <div className="flex flex-col items-center p-8 text-center">
                <div className="mb-5 flex h-14 w-14 items-center justify-center rounded-full border border-[#34E0A1]/20 bg-[#34E0A1]/10">
                  <Check className="text-[#34E0A1]" size={28} strokeWidth={3} />
                </div>
                <h3 className="mb-2 text-lg font-bold text-white tracking-wider">系统访问权限已生成</h3>
                <p style={{color: DIM}} className="mb-6 text-xs">管理员节点已自动复制至剪贴板</p>

                <div className="mb-5 w-full rounded-xl border border-white/5 bg-[#000000] py-4">
                  <p className="mb-1 text-[10px] uppercase tracking-widest text-[#8A93A3]" style={{fontFamily:MONO}}>System Admin ID</p>
                  <p style={{fontFamily: MONO, color: TXT}} className="text-2xl font-bold tracking-wider">{WECHAT_ID}</p>
                </div>

                <div className="mb-8 flex w-full items-start gap-3 rounded-xl border px-4 py-3.5 text-left"
                     style={{borderColor: 'rgba(231,200,132,0.2)', background: 'rgba(231,200,132,0.05)'}}>
                  <AlertCircle size={16} className="mt-0.5 shrink-0" style={{color: GOLD}} />
                  <p className="text-xs leading-relaxed" style={{color: '#BCC2CE'}}>
                    出于严格风控，添加时请务必发送验证口令 <span className="font-bold tracking-widest" style={{color: GOLD}}>Alpha节点</span>，否则您的请求将被系统无视。
                  </p>
                </div>

                <button
                  onClick={() => window.location.href = 'weixin://'}
                  style={{background: GREEN, color: INK}}
                  className="mb-4 flex w-full items-center justify-center gap-2 rounded-xl py-3.5 text-[15px] font-bold transition-transform active:scale-95"
                >
                  打开微信前往验证 <ExternalLink size={16} />
                </button>

                <button
                  onClick={() => setShowModal(false)}
                  className="text-xs tracking-wider text-[#8A93A3] transition-colors hover:text-white"
                >
                  关闭窗口
                </button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
};

export default function App(){
  const scrollRef = useRef(null);
  const [view, setView] = useState('landing');

  return(
    <div style={{background:INK,color:TXT,fontFamily:SANS}} className="relative h-screen w-full overflow-hidden">
      <style>{`@import url('https://fonts.googleapis.com/css2?family=Noto+Serif+SC:wght@400;500;600;700&display=swap');::selection{background:rgba(52,224,161,0.3);}`}</style>
      <div className="pointer-events-none fixed inset-0" style={{background:'radial-gradient(circle at 50% 0%, rgba(52,224,161,0.06), transparent 55%)'}}/>

      {view === 'landing' ? (
        <>
          <ScrollBar scrollRef={scrollRef} />
          <div
            ref={scrollRef}
            className="relative z-10 h-full w-full overflow-y-auto overflow-x-hidden"
            style={{ scrollSnapType: 'y mandatory' }}
          >
            <Hero/>
            <Principle idx="01" zh="捕捉趋势"
              maxim="市场不可预测，规则方可长青。"
              takeaway="不预测，只跟随趋势">
              <MartingaleChart/>
            </Principle>
            <Principle idx="02" zh="控制回撤"
              maxim="先求不死，再求大胜。"
              takeaway="回撤可控，绝不致命">
              <RecoveryChart/>
            </Principle>
            <Principle idx="03" zh="及时止损"
              maxim="截断亏损，让利润奔跑。"
              takeaway="亏有底线，赢无上限">
              <BalanceScale/>
            </Principle>
            <Principle idx="04" zh="穿越牛熊"
              maxim="牛市决定收益，熊市决定复利。"
              takeaway="熊市不亏，牛市起飞">
              <BullBearTabs/>
            </Principle>
            <Principle idx="05" zh="系统驱动"
              maxim="把交易交给系统，把情绪踢出局。"
              takeaway="规则驱动，透明可验">
              <SystematicChart/>
              <RuleStatus/>
            </Principle>
            <Finale onViewRadar={() => setView('radar')}/>
          </div>
        </>
      ) : (
        <div className="relative z-10 h-full w-full overflow-y-auto overflow-x-hidden">
          <AnimatePresence mode="wait">
            <SignalRadar onBack={() => setView('landing')} />
          </AnimatePresence>
        </div>
      )}
    </div>
  );
}