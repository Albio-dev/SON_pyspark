# Frequent_itemset_spark

ToDo SON:
- [X] 1. generare tutti i possibili itemset dal basket 
- [X] 2. ritornare gli itemset frequenti nel basket <br>
        - Filtrare gli itemset frequenti di ogni basket e ritornare solo quelli massimali<br>
        - Vedere ti cominciare il ciclo già dalle coppie
- [X] 3. per ogni itemset candidato contare le occorrenze nel basket
- [X] 4. sommare le occorrenze di ogni basket e calcolare il supporto totale

ToDo progetto:
 - [ ] Far funzionare le funzioni con tuple chiave-valore
 - [X] Controllare la composizione e la suddivisione delle partizioni (che non si rompa con partizioni vuote)(che venga partizionato automaticamente)
 - [X] Distribuire il calcolo della dimensione di ogni partizione (no basket_sizes)
 - [ ] Fare una compilazione degli item in numeri
 - [X] Fare il benchmark tra apriori sequenziale e SON
 - [ ] Provare altri dataset
 - [ ] Confrontare con la funzione di spark (DataFrame.freqItems)
 - [ ] Stilare il report