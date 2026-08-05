**IEEE-CIS Fraud Detection** — i to nie przypadkowy wybór: te dane to transakcje płatnicze Vesty w **e-commerce**, czyli jeden dataset obsługuje obie Twoje narracje (fraud dla fintechu, e-commerce dla tech-commerce). Do tego jest wielotabelowy (transaction + identity), silnie niezbalansowany (~3.5% fraudu), ma anonimizowane cechy wymagające prawdziwej pracy EDA i wyraźny komponent czasowy. Churn (np. KKBox) trzymaj jako drugi projekt, jeśli w ogóle — metodologia jest identyczna, więc krańcowa wartość mała.

**Plan na 4-5 tygodni:**

1. **Tydzień 1 — dane i walidacja.** Join tabel, EDA, ustalenie schematu walidacji **przed** pierwszym modelem: split czasowy (nie losowy), sprawdzenie czy `TransactionDT` pozwala na czysty podział, i decyzja co robić z proxy-identyfikatorami kart (żeby ten sam klient nie był w train i test). To najważniejszy tydzień — tu zapadają decyzje, o które zapytają na rozmowie.

2. **Tydzień 2 — baseline i cechy.** LightGBM na surowych cechach jako baseline, potem point-in-time agregaty: liczba transakcji per karta w oknie 1h/24h/7d, odchylenie kwoty od baseline'u karty, czas od poprzedniej transakcji, velocity na urządzeniu/IP. Każdy agregat liczony tylko z przeszłości — udokumentuj to wprost, bo to najczęstsze źródło leakage w tym zbiorze.

3. **Tydzień 3 — kalibracja i próg.** PR-AUC jako metryka główna (nie ROC-AUC), isotonic vs Platt z reliability diagram, wybór progu pod konkretny budżet false positives (np. „blokujemy max 1% legalnych transakcji"), macierz kosztów. SHAP na koniec — global i kilka case studies pojedynczych decyzji.

4. **Tydzień 4 — produkcja.** FastAPI + Docker, MLflow do trackingu eksperymentów (od początku, nie na końcu), prosty monitoring driftu (PSI na cechach i score'ach) na symulowanym strumieniu z holdoutu. Tu Twoje 15 lat daje przewagę — zrób to lepiej niż typowy kandydat DS.

5. **Tydzień 5 — README i ablacje.** Tabela: baseline vs +features vs +kalibracja, plus jeden nieudany wariant (MLP albo autoencoder) z uczciwym „przegrał o X, dlatego że...". Ten ostatni punkt buduje więcej wiarygodności niż najlepszy wynik.

**Jedna rzecz do zapisania na starcie:** dziennik decyzji (plik `DECISIONS.md`, jedno zdanie per decyzja z uzasadnieniem). Za dwa miesiące, w rozmowie technicznej, nie będziesz pamiętał, dlaczego wybrałeś isotonic — a to jest dokładnie pytanie, które usłyszysz.