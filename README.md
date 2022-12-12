# Partitioned-Hash-Join Part 2
Κατερίνα Μπελιγιάννη Α.Μ. 1115201800126 <br />
Σοφία Κατσαούνη Α.Μ.  1115201800070 <br />
Σπύρος Κάντας Α.Μ. 1115201800059

## Πως να κάνετε compile and execute:
cd main\
chmod +x compile_execute\
./compile_execute <ονομα φακελου εισόδου> π.χ. ./compile_execute ./Tests

## Το πρόγραμμα
Το πρόγραμμα αρχικά διαβάζει τα αρχεία με τα Relation και τα αποθηκεύει
στη δομή Joiner ως μια λίστα από Relation. Έπειτα διαβάζει τα Queries από
αρχείο που του δίνεται και εκτελεί τα Join.

Ενδιάμεση δομή: UsedRelation
  Για ενδιάμεση δομή έχουμε επιλέξει έναν πίνακα από πίνακες ακεραίων(MatchRow)
  που ο καθένας έχει όσα στοιχεία είναι και οι σχέσεις στο query που εκτελείται.
  (πχ αν συμμετέχουν 4 relations στο query, η αρχική δομή θα είναι: <br />
  -1 -1 -1 -1 <br />
  -1 -1 -1 -1 <br />
  -1 -1 -1 -1 <br />
  -1 -1 -1 -1 <br />
  ...) <br />
  Η πρώτη στήλη αναπαριστά το relation με index 0, η δεύτερη με index ένα etc.
  Κάθε φορά που θέλουμε να κάνουμε ένα join, δημιουργούμε ένα RelCol το οποίο είναι
  ένας πίνακας με Tuples όπου key=rowid, payload=value, από την αντίστοιχη στήλη του
  στο usedRelations. Αφού τελειώσει το join, επιστρέφεται ένας πίνακας(Matches) από Tuples
  και στην συνέχεια φιλτράρουμε τον πίνακα usedRelations και τον ανανεώνουμε.

### Join και ανανέωση ενδιάμεσης δομής:
  \* Μια στήλη ενός πίνακα προς join φορτώνεται στην ειδική δομή RelColumn
   που είναι ένας πίνακας με Tuples όπου key=rowid, payload=value, δομή που
   χρησιμοποιείται από την PartitionHashJoin.

  - First Join: οι πίνακες προς join φορτώνονται\* από τις αρχικές σχέσεις και δίνονται στην
   PartitionHashJoin. Τα αποτελέσματα που επιστρέφονται είναι tuple των 2 που κάθε tuple έχει το
   ζευγάρι row id που ταίριαξε. Τα αποτελέσματα απλά εισχωρούνται στην ενδιάμεση δομή.

  - Left/Right Join: η αριστερή/δεξιά σχέση υπάρχει στον ενδιάμεσο πίνακα όμως η άλλη όχι,
   σε αυτή τη περίπτωση φορτώνεται\* ο πίνακας που υπάρχει στο πίνακα και ο άλλος φορτώνεται\* από
   την αρχική σχέση. Τα αποτελέσματα είναι tuple όπως παραπάνω. Έπειτα γίνεται ανανέωση του ενδιάμεσου
   πίνακα. Για κάθε τιμή του πίνακα στην ενδιάμεση δομή γίνεται έλεγχος για το αν υπάρχει στην αντίστοιχη
   στήλη στα αποτελέσματα αν υπάρχει κρατείται και προστίθενται όλα τα αποτελέσματα με αυτή τη στήλη.
   Αν δεν υπάρχει διαγράφεται η γραμμή αυτή της ενδιάμεσης δομής.

  - Self/Filter Join: η σχέση φορτώνεται\* από την αρχική ή την ενδιάμεση αναλόγως αν υπάρχει. Σε αυτές
   τις περιπτώσεις η διαφορά είναι ότι τα αποτελέσματα που επιστρέφει είναι μία στήλη(SingleCol). Η
   ενδιάμεση δομή σκανάρεται και τα στοιχεία που υπάρχουν και στα αποτελέσματα μένουν ενώ τα άλλα διαγράφονται.


### Joiner:
  Η κλάση Joiner είναι υπεύθυνη για τη εκτέλεση πολλών join όπως τα ορίζει
  το query, τα υλοποιεί χρησιμοποιώντας τη PartitionHashJoin (βλέπε Part 1)

### Relation:
  Η κλάση Relation φορτώνει και αποθηκεύει χαρακτηριστικά του πίνακα όπως size, αριθμό
  στηλών και έναν πίνακα με τις στήλες του.

### Parser:
  Ο Parser είναι υπεύθυνος για την σάρωση ανάλυση και μετατροπή αρχείου με queries στις
  αντίστοιχες ειδικές δομές Query, Predicates, Projection.
