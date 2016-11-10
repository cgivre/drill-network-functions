=======
# Drill Network Functions
This library contains a collection of network-related functions for Apache Drill. It includes:

* **`inet_aton(<IPv4>)`**:  This function converts an IPv4 address in dotted decimal notation into an integer.  This is useful for sorting IP addresses, and reducing the amount of space that they take on disk.

* **`inet_ntoa(<int>)`**: This function returns an IP in dotted decimal notation given its integer representation. 

* **`is_private_ip(<IPv4>)`**:  Returns true if the IP address is private.

* **`in_network( <IPv4>, <CIDR Block>)`**: Retunrs true if the IPv4 address is in the CIDR Block

* **`getAddressCount( <CIDR Block> )`**: Returns the number of IP addresses in a given CIDR Block

* **`getBroadcastAddress( <CIDR Block> )`**:  Returns the broadcast address in dotted decimal notation from a given CIDR block.

* **`getNetmask( <CIDR Block> )`**:  Returns the netmask for a given CIDR Block

* **`getLowAddress( <CIDR Block> )`**:  Returns the first IPv4 address in dotted decimal notation for a given CIDR Block

* **`getHighAddress( <CIDR Block> )`**:  Returns the last IPv4 address in dotted decimal notation for a given CIDR Block

## Installing These Functions
This collection of functions does not have any dependencies that are not already included in Drill.  You can build the functions from source by cloning this repository, navigating to the directory and typing: 
`mvn clean package -DskipTests`.
Once you've done that, you'll find two `.jar` files in the `target/` folder.  Copy both these files to `<drill path>/jars/3rdParty`.

Alternatively, you can download the jar files here: https://github.com/cgivre/drill-network-functions/releases and copy them to `<drill path>/jars/3rdParty`.
