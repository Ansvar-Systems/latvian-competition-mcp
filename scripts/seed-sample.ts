/**
 * Seed the CC (Competition Council of Latvia) database with sample decisions,
 * mergers, and sectors for testing.
 *
 * Usage:
 *   npx tsx scripts/seed-sample.ts
 *   npx tsx scripts/seed-sample.ts --force
 */

import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["CC_LV_DB_PATH"] ?? "data/cc-lv.db";
const force = process.argv.includes("--force");

const dir = dirname(DB_PATH);
if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted existing database at ${DB_PATH}`); }

const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

const sectors = [
  { id: "energy", name: "Enerģētika", name_en: "Energy", description: "Elektroenerģijas ražošana, pārvade un tirdzniecība, dabasgāze un atjaunojamā enerģija.", decision_count: 3, merger_count: 1 },
  { id: "retail", name: "Mazumtirdzniecība", name_en: "Retail", description: "Pārtikas mazumtirdzniecība, tirdzniecības centri un e-komercija.", decision_count: 2, merger_count: 2 },
  { id: "telecommunications", name: "Telekomunikācijas", name_en: "Telecommunications", description: "Mobilo tīklu pakalpojumi, platjoslas internets un televīzijas izplatīšana.", decision_count: 2, merger_count: 1 },
  { id: "financial_services", name: "Finanšu pakalpojumi", name_en: "Financial services", description: "Komerciālās bankas, apdrošināšana un maksājumu pakalpojumi.", decision_count: 1, merger_count: 1 },
  { id: "transport", name: "Transports", name_en: "Transport", description: "Kravas pārvadājumi, publiskais transports un loģistika.", decision_count: 2, merger_count: 0 },
];

const is = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) is.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

const decisions = [
  { case_number: "P/04/2023", title: "Latvenergo — dominējošā stāvokļa ļaunprātīga izmantošana elektroenerģijas mazumtirdzniecībā", date: "2023-05-30", type: "abuse_of_dominance", sector: "energy", parties: JSON.stringify(["Latvenergo AS"]), summary: "KP konstatēja, ka Latvenergo AS ļaunprātīgi izmantoja dominējošo stāvokli elektroenerģijas mazumtirdzniecības tirgū, piemērojot diskriminējošus nosacījumus patērētājiem, kas pāriet pie alternatīviem tirgotājiem.", full_text: "KP uzsāka lietu pret Latvenergo AS pēc vairāku komersantu sūdzībām. Latvenergo ir dominējošs tirgotājs elektroenerģijas mazumtirdzniecības tirgū ar tirgus daļu virs 50%. KP konstatēja, ka uzņēmums piemēroja nepamatoti ilgus savienojuma termiņus un sarežģītas administratīvās procedūras patērētājiem, kuri vēlējās mainīt piegādātāju. Turklāt Latvenergo nenodrošināja pietiekamu cenu pārredzamību. KP uzlika naudas sodu un uzdeva labot praksi.", outcome: "fine", fine_amount: 2_100_000, gwb_articles: JSON.stringify(["13", "35"]), status: "final" },
  { case_number: "P/07/2022", title: "Rimi Baltic / Maxima — koordinētas cenu noteikšanas izmeklēšana pārtikas mazumtirdzniecībā", date: "2022-09-20", type: "cartel", sector: "retail", parties: JSON.stringify(["Rimi Baltic SIA", "Maxima Latvija SIA"]), summary: "KP izmeklēja koordinētu cenu noteikšanu starp vadošajām pārtikas mazumtirdzniecības ķēdēm, aizdomājoties par saskaņotu rīcību noteiktu produktu kategorijās.", full_text: "KP veica pārbaudes divās lielākajās pārtikas mazumtirdzniecības ķēdēs Latvijā pēc tirgus uzraudzības datiem, kas liecināja par paralēlām cenu izmaiņām vienlaicīgi un identiskos apmēros noteiktās produktu kategorijās. KP nepierādīja formālu karteļvienošanos, taču konstatēja, ka uzņēmumi sistemātiski apmainījās ar komerciāli jutīgu informāciju par cenu izmaiņu plāniem. KP izdeva norādījumus par komerciālās informācijas apmaiņas pārtraukšanu un ierosināja Konkurences likuma grozījumus par cenu signalizāciju.", outcome: "cleared_with_conditions", fine_amount: null, gwb_articles: JSON.stringify(["11", "13"]), status: "final" },
  { case_number: "P/02/2023", title: "Lursoft — dominējošā stāvokļa izmantošana uzņēmumu informācijas datubāzēs", date: "2023-08-14", type: "abuse_of_dominance", sector: "financial_services", parties: JSON.stringify(["Lursoft IT SIA"]), summary: "KP izmeklēja Lursoft IT SIA iespējamu dominējošā stāvokļa ļaunprātīgu izmantošanu uzņēmumu reģistra informācijas izplatīšanas tirgū, liekot šķēršļus alternatīvo pakalpojumu sniedzēju darbībai.", full_text: "KP izmeklēja sūdzību par Lursoft IT praksi uzņēmumu reģistra datu izplatīšanas tirgū. Lursoft ir dominējošs uzņēmumu informācijas datu bāzu pakalpojumu sniedzējs Latvijā. KP konstatēja, ka uzņēmums ir izmantojis savu privileģēto piekļuvi Uzņēmumu reģistra datiem, lai radītu šķēršļus alternatīvajiem datu agregatoriem. KP uzdeva nodrošināt nediskriminējošu piekļuvi Uzņēmumu reģistra datiem atbilstoši Datu atkalizmantošanas direktīvai.", outcome: "cleared_with_conditions", fine_amount: null, gwb_articles: JSON.stringify(["13", "PSI"]), status: "final" },
  { case_number: "P/05/2022", title: "Transporta nozares sektorālā izmeklēšana — kravas pārvadājumi", date: "2022-06-15", type: "sector_inquiry", sector: "transport", parties: JSON.stringify(["Latvijas dzelzceļš AS", "LDz Cargo SIA"]), summary: "KP publicēja sektorālās izmeklēšanas rezultātus dzelzceļa kravas pārvadājumu tirgū, konstatējot struktūrālas problēmas konkurencei.", full_text: "KP veica sektorālo izmeklēšanu dzelzceļa kravas pārvadājumu tirgū. Latvijas dzelzceļš AS kontrolē dzelzceļa infrastruktūru, bet LDz Cargo SIA ir dominējošais kravas pārvadātājs. KP konstatēja problēmas infrastruktūras piekļuvē alternatīviem pārvadātājiem. Tika rekomendēts: nodrošināt skaidrāku ceļa maksu struktūru; uzlabot diskriminācijas aizlieguma ieviešanu; veicināt jaunu tirgus dalībnieku ienākšanu.", outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["20a"]), status: "final" },
  { case_number: "P/01/2024", title: "Telecom tirgus — MVNO piekļuves izmeklēšana", date: "2024-02-20", type: "abuse_of_dominance", sector: "telecommunications", parties: JSON.stringify(["LMT AS"]), summary: "KP izmeklēja LMT AS praksi attiecībā uz piekļuves nosacījumiem virtuālo mobilo tīklu operatoriem (MVNO) Latvijā.", full_text: "KP saņēma sūdzību no MVNO operatora par LMT noteiktajiem piekļuves nosacījumiem. LMT AS ir lielākais mobilo sakaru operators Latvijā ar tirgus daļu virs 40%. KP konstatēja, ka LMT ir piemērojis diskriminējošus vairumtirdzniecības tarifus MVNO operatoriem salīdzinot ar saviem mazumtirdzniecības tarifiem. KP uzdeva LMT nodrošināt MVNO nediskriminējošu un pārredzamu piekļuvi tīklam saskaņā ar regulatora prasībām.", outcome: "fine", fine_amount: 890_000, gwb_articles: JSON.stringify(["13", "15"]), status: "final" },
];

const id = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
db.transaction(() => { for (const d of decisions) id.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); })();
console.log(`Inserted ${decisions.length} decisions`);

const mergers = [
  { case_number: "C/01/2024", title: "Rimi Baltic — mazumtirdzniecības veikalu tīkla paplašināšana", date: "2024-01-22", sector: "retail", acquiring_party: "Rimi Baltic SIA", target: "Top! veikalu tīkls", summary: "KP apstiprināja I fāzē mazumtirdzniecības veikalu iegādi, konstatējot ierobežotu ģeogrāfisko pārklāšanos.", full_text: "KP izskatīja koncentrāciju, kur Rimi Baltic iegūst Top! mazumtirdzniecības veikalu tīklu Latvijā. Rimi Baltic darbojas vairāk nekā 70 veikalos. Top! tīklam ir 18 veikali. KP veica lokālo tirgu analīzi un konstatēja, ka šo veikalu ģeogrāfiskā pārklāšanās ir ierobežota. Koncentrācija apstiprināta bez nosacījumiem.", outcome: "cleared_phase1", turnover: 750_000_000 },
  { case_number: "C/03/2023", title: "Banku sektors — apdrošināšanas uzņēmuma iegāde", date: "2023-07-11", sector: "financial_services", acquiring_party: "Citadele Banka AS", target: "Compensa Life Latvia AAS", summary: "KP apstiprināja ar nosacījumiem apdrošināšanas uzņēmuma iegādi banku grupā, prasot nodrošināt nediskriminējošu piekļuvi konkurentiem.", full_text: "KP izskatīja Citadele Banka iegūstot Compensa Life Latvia apdrošināšanas uzņēmumu. Citadele ir viens no lielākajiem Baltijas reģiona bankām ar plašu filiāļu tīklu Latvijā. KP identificēja bankassurance problēmas — kombinējot banku izplatīšanu ar pašu apdrošināšanu, var tikt izslēgti konkurējoši apdrošinātāji. KP nosacīja apstiprinājumu ar prasību Citadele nepiesaistīt savus kredītu klientus tikai pie Compensa produktiem.", outcome: "cleared_with_conditions", turnover: 1_850_000_000 },
  { case_number: "C/02/2023", title: "Telekomunikāciju infrastruktūra — optisko šķiedru tīkla iegāde", date: "2023-03-28", sector: "telecommunications", acquiring_party: "Tet AS", target: "Lattelecom infrastruktūras aktīvi", summary: "KP apstiprināja I fāzē optisko šķiedru tīkla aktīvu iegādi, konstatējot, ka darījums nerada ievērojamas konkurences problēmas platjoslas tīklu tirgū.", full_text: "KP izskatīja Tet AS iegūstot Lattelecom optisko šķiedru infrastruktūras aktīvus Latvijā. Tet AS ir lielākais fiksētās telekomunikāciju operatora Latvijā. Iegūtie aktīvi ir daļa no esošo pakalpojumu infrastruktūras. KP novērtēja, ka darījums nerada jaunas vertikālas vai horizontālas tirgus problēmas, jo Tet jau sniedz pakalpojumus šajā infrastruktūrā. Koncentrācija apstiprināta bez nosacījumiem I fāzē.", outcome: "cleared_phase1", turnover: 620_000_000 },
];

const im = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
db.transaction(() => { for (const m of mergers) im.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); })();
console.log(`Inserted ${mergers.length} mergers`);

const dCount = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mCount = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sCount = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log(`\nDatabase summary:\n  Sectors:   ${sCount}\n  Decisions: ${dCount}\n  Mergers:   ${mCount}\n\nDone. Database ready at ${DB_PATH}`);
db.close();
