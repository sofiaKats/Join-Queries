# Partitioned-Hash-Join
Σπύρος Κάντας Α.Μ. 1115201800059
Κατερίνα Μπελιγιάννη Α.Μ. 1115201800126
Σοφία Κατσαούνη Α.Μ.  1115201800070

### How to compile and execute:
cd main\
chmod +x compile_execute\
./compile_execute <ονομα φακελου εισόδου> π.χ. ./compile_execute ./Tests

ή

cd main\
make\
execute with: ./program ./Tests\
make clean

Το πρόγραμμα χωρίζεται σε δύο φάσεις:

1) Partition-phase
2) Hopscotch

Partition-phase:
  Part : struct με Relation και το PrefixSum του.

  Σε αυτή τη φάση η PartitionedHashJoin.Solve καλεί την PartitionRec η οποία
  πρώτα σπάει με την κλάση Partition το Relation σε μικρότερα Relation με το
  PrefixSum (Parts) και για κάθε ένα αναδρομικά το ξανασπάει.

  Η Partition παίρνει σαν ορίσματα το Relation το Ν το from και to. Το from
  και to είναι για να καθορίσουν πιο κομμάτι του Relation μας ενδιαφέρει για
  partitioning. Κατα το PartitionRec δίνεται αναδρομικά όλο το Relation και
  στα from to τα index από το PrefixSum.

  Όταν φτάσει στο τελευταίο στάδιο Partitioning καλεί την Merge η οποία ενώνει
  τα Parts σε έναν ενιαίο πίνακα Relation και έναν PrefixSum.

  Η Merge παίρνει το Relation PrefixSum (Part) και from που δείχνει σε ποια θέση
  ξεκινάει το Relation που της δώσαμε. Για το PrefixSum ψάχνει να βρεί κενή θέση
  για να ξεκινήσει από εκεί κρατώντας το προηγούμενο value σαν βάση και συνεχίζοντας
  την πρόσθεση από εκεί.

  Η PartitionRec επιστρέφει τον αριθμό των passes που πέρασε τον αρχικό πίνακα
  ώστε να είναι ο ίδιος και στα δύο Relations.


Hopscotch:
  Index: κλάση που αναπαριστά κάθε bucket του hashtable, όπου κρατάει το tuple
  και ένα bitmap.

  Hashtable:  Πίνακας από δείκτες σε Index όπου υλοποιεί τις λειτουργίες του πίνακα
  κατακερματισμού με βάση τον αλγόριθμο του hopscotch hashing.

    hash:
      Χρησιμοποιείται συνάρτηση που κάνει hash τα n lsb τα οποία αυξάνονται όταν
      γίνεται rehash. Για αυτό και το μέγεθος του πίνακα είναι δύναμη το 2, συγκεκριμένα
      η μεγαλύτερη δύναμη του 2 πιο κοντά στο μέγεθος του αντίστοιχου Relation.
    H:
      Έχει την τιμή 8, (εκτός αν το μέγεθος του πίνακα είναι μικρότερο).
    add:
      Ελέγχει αν πρέπει να γίνει resize ο πίνακας και καλεί την insert.
    insert:
      Βρίσκει μέσω την findPos την κατάλληλη θέση για την tuple και την προσθέτει.
    findPos:
      Αφού ελεγχθεί αν υπάρχει ελεύθερη θέση είτε στην θέση που έγινε hash η tuple,
      είτε σε μία από τις γειτονιές της, καλείται η slideLeft όπου ελέγχει τις Η-1 θέσεις
      για το κατάλληλο στοιχείο για αντικατάσταση j και αν βρεθεί μέσω της findSwapNeighbourPos,
      ανταλλάζει την κενή θέση με το j όσες φορές χρειαστεί. Άν δεν μπορεί να βρεθεί τέτοιο
      στοιχείο γίνεται resize.


Parser:
  ReadRelations:
    Διαβάζει ένα ένα τα αρχεία του φακέλου που δώθηκε, δημιουργεί Relations και τις αποθηκεύει
    σε πίνακα.

  ReadQueries:
    Διαβάζει ένα ένα τα ζευγάρια των queries και τα αποθηκεύει σε έναν πίνακα. Μετά για κάθε
    ζευγάρι καλείται η PartitionedHashJoin.Solve.
