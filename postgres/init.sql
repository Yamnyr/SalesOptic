CREATE TABLE clients (
    id INT PRIMARY KEY,
    nom TEXT,
    pays TEXT,
    segment TEXT
);

INSERT INTO clients (id, nom, pays, segment) VALUES
(1, 'Alice Dupont', 'France', 'Premium'),
(2, 'Bob Schmidt', 'Allemagne', 'Standard'),
(3, 'Charlie Martin', 'France', 'Standard'),
(4, 'David Smith', 'USA', 'Premium'),
(5, 'Emma Wilson', 'UK', 'Premium'),
(6, 'Francesco Rossi', 'Italie', 'Standard'),
(7, 'Greta Nilsen', 'Norvège', 'Premium'),
(8, 'Hans Müller', 'Allemagne', 'Standard'),
(9, 'Isabella Garcia', 'Espagne', 'Premium'),
(10, 'Jean Lefebvre', 'France', 'Standard'),
(11, 'Kevin Lee', 'Canada', 'Standard'),
(12, 'Laura Costa', 'Portugal', 'Premium'),
(13, 'Mikhail Volkov', 'Russie', 'Standard'),
(14, 'Nora Van der Berg', 'Pays-Bas', 'Premium'),
(15, 'Oliver Brown', 'Australie', 'Standard'),
(16, 'Pierre Durand', 'France', 'Standard'),
(17, 'Quentin Leroy', 'Belgique', 'Premium'),
(18, 'Rose Miller', 'USA', 'Standard'),
(19, 'Sven Forsberg', 'Suède', 'Premium'),
(20, 'Takashi Sato', 'Japon', 'Standard');
