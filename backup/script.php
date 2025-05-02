<?php
$host = 'localhost';
$username = 'root';
$password = '';
$database = 'nguyenkim-autozalo';

$sqlFile = 'C:\\nguyenkim-autozalo.sql';

$conn = new mysqli($host, $username, $password, $database);

if ($conn->connect_error) {
    die("Err: " . $conn->connect_error);
}

$handle = fopen($sqlFile, 'r');
if ($handle === false) {
    die("Err: " . $sqlFile);
}

echo "Start...\n";

$sql = '';
while (($line = fgets($handle)) !== false) {
    $line = trim($line);
    if (empty($line) || substr($line, 0, 2) === '--' || substr($line, 0, 1) === '#') {
        continue;
    }

    $sql .= $line;

    if (substr($line, -1) === ';') {
        if ($conn->query($sql) === false) {
            echo "Err: " . $conn->error . "\n";
        }
        $sql = '';
    }
}

fclose($handle);

$conn->close();

echo "Import hoàn tất!\n";
?>