package bdm.landingzone;

import bdm.landingzone.persistenceloaders.RentalsPersistenceLoader;

public class Main {

    public static void main(String[] args) {
        System.out.println("Running landing zone...");

        System.out.println("Creating rentals persistence loader...");
        RentalsPersistenceLoader loader = new RentalsPersistenceLoader(
                "mongodb://localhost:27017",
                "persistentLanding",
                "rentals"
        );
        System.out.println("Loading rentals into persistence zone...");
        loader.loadRentalsFromJsonFiles("resources/idealista/");

        System.out.println("Landing zone run successfully!");
    }
}