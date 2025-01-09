#!/usr/bin/env bash

export LC_CTYPE=en_US.UTF-8
export LC_ALL=en_US.UTF-8

ARGS_LINUX=$@
LOG_FILE=/tmp/install-yb-voyager.log
CHECK_ONLY_DEPENDENCIES="false"
FORCE_INSTALL="false"
PRINT_DEPENDENCIES="false"

centos_yum_package_requirements=(
  "make|min|0"
  "sqlite|min|0"
  "perl|min|0"
  "perl-DBI|min|0"
  "perl-ExtUtils-MakeMaker|min|0"
  "mysql-devel|min|0"
  "libaio|min|0"
  "oracle-instantclient-tools|exact|21.5.0.0.0"
  "oracle-instantclient-basic|exact|21.5.0.0.0"
  "oracle-instantclient-devel|exact|21.5.0.0.0"
  "oracle-instantclient-jdbc|exact|21.5.0.0.0"
  "oracle-instantclient-sqlplus|exact|21.5.0.0.0"
)

ubuntu_apt_package_requirements=(
  "make|min|0"
  "sqlite3|min|0"
  "perl|min|0"
  "libdbi-perl|min|0"
  "libaio1|min|0"
  "libmysqlclient-dev|min|0"
  "libmodule-build-perl|min|0"
  "oracle-instantclient-tools|exact|21.5.0.0.0"
  "oracle-instantclient-basic|exact|21.5.0.0.0"
  "oracle-instantclient-devel|exact|21.5.0.0.0"
  "oracle-instantclient-jdbc|exact|21.5.0.0.0"
  "oracle-instantclient-sqlplus|exact|21.5.0.0.0"
  "oracle-instantclient12.1-basic|min|0"
)

# Array with format "Module::Name|requirement_type|required_version|tarball_name"
cpan_modules_requirements=(
  "Test::Deep|min|0|Test-Deep-1.204.tar.gz"
  "Capture::Tiny|min|0|Capture-Tiny-0.48.tar.gz"
  "Mock::Config|min|0.02|Mock-Config-0.03.tar.gz"
  "Test::NoWarnings|min|1.06|Test-NoWarnings-1.06.tar.gz"
  "String::Random|min|0|String-Random-0.32.tar.gz"
  "IO::Compress::Base|min|0|IO-Compress-2.213.tar.gz"
)

ubuntu_missing_apt_packages=()
centos_missing_yum_packages=()
missing_cpan_modules=()
binutils_wrong_version=0
java_wrong_version=0
pg_dump_wrong_version=0
pg_restore_wrong_version=0
psql_wrong_version=0

trap on_exit EXIT

on_exit() {
	rc=$?
	set +x
	if [ $rc -ne 0 ]
    then
		echo "Script failed. Check log file ${LOG_FILE} ."
		if [[ ${ON_INSTALLER_ERROR_OUTPUT_LOG:-N} = "Y" ]]
		then
			sudo cat ${LOG_FILE}
		fi
	fi
}

main() {
    set -x

	os=$(uname -s)
	case ${os} in
		"Linux")
			get_passed_options
			# Proceed to distribution check.
			;;
		*)
			echo -e "\e[31mERROR: unsupported os ${os}\e[0m"
			exit 1
			;;
	esac

	# Linux
	dist=$(cat /etc/os-release | grep -w NAME | awk -F'=' '{print $2}' | tr -d '"')
	case ${dist} in
		"CentOS Stream")
			centos_main
			;;
		"CentOS Linux")
			centos_main
			;;
		"AlmaLinux")
			centos_main
			;;
		"Red Hat Enterprise Linux")
			centos_main
			;;
		"Red Hat Enterprise Linux Server")
			centos_main
			;;
		"Ubuntu")
			ubuntu_main
			;;
		*)
			echo -e "\e[31mERROR: unsupported linux distribution: ${dist}\e[0m"
			exit 1
			;;
	esac
    # Check if yb-voyager is installed using yb-voyager version. Else exit with error and log it too.
    yb_voyager_version=$(yb-voyager version)
    if [ $? -ne 0 ]; then
        echo -e "\e[31mERROR: yb-voyager did not get installed.\e[0m"
        exit 1
    else
        echo "yb-voyager version"
        echo "$yb_voyager_version"
    fi
}

#=============================================================================
# COMMON
#=============================================================================

output() {
	set +x
	echo "$@"
	>&2 echo "$@"
	set -x
}

# Function to install a Perl module from a tar.gz package
install_perl_module() {
    local module_name="$1"
    local requirement_type="$2"
    local required_version="$3"
    local package="$4"

    # Check if the module is already installed and meets the version requirements
    check_perl_module_version "$module_name" "$requirement_type" "$required_version" "true"
    if [[ $? -eq 0 ]]; then
        return
    fi
    
    # Extract the package
    tar -xzvf "$package" 1>&2 || { echo "Error: Failed to extract $package"; exit 1; }
    
    # Get the extracted directory name (remove the .tar.gz extension)
    local dir_name="${package%.tar.gz}"
    # If tar zip is like .tgz then remove .tgz extension. Add a condition for that
    dir_name="${dir_name%.tgz}"
    
    # Navigate to the extracted directory
    cd "$dir_name" || { echo "Error: Failed to enter directory $dir_name"; exit 1; }
    
    # Check if Makefile.PL or Build.PL exists and run the appropriate commands
    if [[ -f Makefile.PL ]]; then
        perl Makefile.PL 1>&2 || { echo "Error: perl Makefile.PL failed for $module_name"; exit 1; }
        make 1>&2 || { echo "Error: make command failed for $module_name"; exit 1; }
        make test 1>&2 || { echo "Error: make test failed for $module_name"; exit 1; }
        sudo make install 1>&2 || { echo "Error: sudo make install failed for $module_name"; exit 1; }
    elif [[ -f Build.PL ]]; then
        perl Build.PL 1>&2 || { echo "Error: perl Build.PL failed for $module_name"; exit 1; }
        ./Build 1>&2 || { echo "Error: ./Build command failed for $module_name"; exit 1; }
        ./Build test 1>&2 || { echo "Error: ./Build test failed for $module_name"; exit 1; }
        sudo ./Build install 1>&2 || { echo "Error: sudo ./Build install failed for $module_name"; exit 1; }
    else
        echo "Error: No Makefile.PL or Build.PL found in $dir_name, installation failed."
        exit 1
    fi

    # Return to the original directory
    cd ..
    
    # Verification of the installed module
    check_perl_module_version "$module_name" "$requirement_type" "$required_version" "false" 
    if [[ $? -ne 0 ]]; then
        exit 1
    fi
}

check_perl_module_version() {
    local module_name="$1"
    local requirement_type="$2"
    local required_version="$3"
    local check_only="$4"  # If "true", suppress error messages and exit silently

    # Get installed version
    local installed_version
    installed_version=$(perl -M"$module_name" -e 'print $'"$module_name"'::VERSION' 2> /dev/null)

    if [[ -z "$installed_version" ]]; then
        if [[ "$check_only" != "true" ]]; then
            echo "Error: $module_name could not be loaded or found."
        fi
        return 1
    fi

    # Version comparison based on requirement type
    if [[ "$requirement_type" == "min" ]]; then
        # Check if installed version is at least the required version
        if [[ $(echo -e "$installed_version\n$required_version" | sort -V | head -n1) == "$required_version" ]]; then
            return 0
        fi
        if [[ "$check_only" != "true" ]]; then
            echo "Error: Installed version of $module_name ($installed_version) does not meet the minimum required version ($required_version)."
        fi
        return 1
    elif [[ "$requirement_type" == "exact" ]]; then
        # Check if installed version matches the required version exactly
        if [[ "$installed_version" == "$required_version" ]]; then
            return 0  
        fi
        if [[ "$check_only" != "true" ]]; then
            echo "Error: Installed version of $module_name ($installed_version) does not match the exact required version ($required_version)."
        fi
        return 1
    else
        echo "Error: Unknown requirement type '$requirement_type' for $module_name."
        exit 1
    fi
}


check_binutils_version() {
	min_required_version='2.25'

	# Example output of "ld -v" on CentOS/RHEL:
	# GNU ld version 2.30-113.el8
	# Example output of "ld -v" on Ubuntu:
	# GNU ld (GNU Binutils for Ubuntu) 2.38
	version=$(ld -v | awk '{print $NF}' | awk -F '-' '{print $1}')
    if [ -z "$version" ]; then
        echo ""
        echo -e "\e[31mERROR: binutils not found. Please install binutils version > ${min_required_version}.\e[0m"
        binutils_wrong_version=1
        return
    fi

	version_ok=$(version_satisfied "$min_required_version" "$version")
	if [[ $version_ok -eq 0 ]]
	then
        echo ""
		echo -e "\e[31mERROR: unsupported binutils version ${version}. Update to binutils version > ${min_required_version}.\e[0m"
		binutils_wrong_version=1
	fi

    echo ""
    output "Found sufficient binutils version = ${version}."
}

# https://stackoverflow.com/a/4025065
# 0 if equal, 1 if $1 > $2, 2 if $2 > $1
vercomp () {
    if [[ $1 == $2 ]]
    then
        echo 0; return;
    fi
    local IFS=.
    local i ver1=($1) ver2=($2)
    # fill empty fields in ver1 with zeros
    for ((i=${#ver1[@]}; i<${#ver2[@]}; i++))
    do
        ver1[i]=0
    done
    for ((i=0; i<${#ver1[@]}; i++))
    do
        if [[ -z ${ver2[i]} ]]
        then
            # fill empty fields in ver2 with zeros
            ver2[i]=0
        fi
        if ((10#${ver1[i]} > 10#${ver2[i]}))
        then
            echo 1; return;
        fi
        if ((10#${ver1[i]} < 10#${ver2[i]}))
        then
            echo 2; return;
        fi
    done
    echo 0; return;
}

# usage: version_satisfied <minimum_version_required> <actual_version>
version_satisfied() {
    res=$(vercomp $1 $2)
    case $res in
        0) echo 1; return;;
        1) echo 0; return;;
        2) echo 1; return;;
    esac
}

check_java() {
	if [ -z "$JAVA_HOME" ]; then
		JAVA_BINARY="java"
	else
		JAVA_BINARY="$JAVA_HOME/bin/java"
	fi

    MIN_REQUIRED_MAJOR_VERSION='17'
	JAVA_COMPLETE_VERSION=$(${JAVA_BINARY} -version 2>&1 | awk -F '"' '/version/ {print $2}')
    JAVA_MAJOR_VER=$(echo "${JAVA_COMPLETE_VERSION}" | awk -F. '{print $1}')

    if ([ -n "$JAVA_MAJOR_VER" ] && (( 10#${JAVA_MAJOR_VER} >= 10#${MIN_REQUIRED_MAJOR_VERSION} )) ) #integer compare of versions.
    then
        echo ""
        output "Found sufficient java version = ${JAVA_COMPLETE_VERSION}"
    else
        echo ""
        echo -e "\e[31mERROR: Java not found or insufficient version ${JAVA_COMPLETE_VERSION}. Please install java>=${MIN_REQUIRED_MAJOR_VERSION}\e[0m"
        java_wrong_version=1
    fi
}

get_passed_options() {
	OPTS=$(getopt -o "dfh", --long check-only-dependencies,force-install,help --name 'install-voyager-airgapped' -- $ARGS_LINUX)

	eval set -- "$OPTS"

	while true; do
		case "$1" in
			-d | --check-only-dependencies ) 
				CHECK_ONLY_DEPENDENCIES="true";
				shift
				;;
            -f | --force-install )
                FORCE_INSTALL="true"
				shift
				;;
            -h | --help )
                echo "Usage: $0 [options]"
                echo "Options:"
                echo "  -d, --check-only-dependencies  Check only dependencies and exit."
                echo "  -f, --force-install            Force install packages without checking dependencies."
                echo "  -h, --help                     Display this help message."
                PRINT_DEPENDENCIES="true"
                shift
                ;;
			* ) 
				break 
				;;
		esac
	done
}

check_pg_dump_and_pg_restore_version() {
    # Check if pg_dump and pg_restore are installed and their major versions are greater than min required version
    MIN_REQUIRED_MAJOR_VERSION='14'
    PG_DUMP_VERSION=$(pg_dump --version | awk '{print $3}')
    PG_RESTORE_VERSION=$(pg_restore --version | awk '{print $3}')
    PSQL_VERSION=$(psql --version | awk '{print $3}')
    PG_DUMP_MAJOR_VER=$(echo "${PG_DUMP_VERSION}" | awk -F. '{print $1}')
    PG_RESTORE_MAJOR_VER=$(echo "${PG_RESTORE_VERSION}" | awk -F. '{print $1}')
    PSQL_MAJOR_VER=$(echo "${PSQL_VERSION}" | awk -F. '{print $1}')

    # Check them separately
    if ([ -n "$PG_DUMP_MAJOR_VER" ] && (( 10#${PG_DUMP_MAJOR_VER} >= 10#${MIN_REQUIRED_MAJOR_VERSION} )) ) #integer compare of versions.
    then
        echo ""
        output "Found sufficient pg_dump version = ${PG_DUMP_VERSION}"
    else
        echo ""
        echo -e "\e[31mERROR: pg_dump not found or insufficient version ${PG_DUMP_VERSION}. Please install pg_dump>=${MIN_REQUIRED_MAJOR_VERSION}\e[0m"
        pg_dump_wrong_version=1
    fi

    if ([ -n "$PG_RESTORE_MAJOR_VER" ] && (( 10#${PG_RESTORE_MAJOR_VER} >= 10#${MIN_REQUIRED_MAJOR_VERSION} )) ) #integer compare of versions.
    then
        echo ""
        output "Found sufficient pg_restore version = ${PG_RESTORE_VERSION}"
    else
        echo ""
        echo -e "\e[31mERROR: pg_restore not found or insufficient version ${PG_RESTORE_VERSION}. Please install pg_restore>=${MIN_REQUIRED_MAJOR_VERSION}\e[0m"
        pg_restore_wrong_version=1
    fi

    if ([ -n "$PSQL_MAJOR_VER" ] && (( 10#${PSQL_MAJOR_VER} >= 10#${MIN_REQUIRED_MAJOR_VERSION} )) ) #integer compare of versions.
    then
        echo ""
        output "Found sufficient psql version = ${PSQL_VERSION}"
    else
        echo ""
        echo -e "\e[31mERROR: psql not found or insufficient version ${PSQL_VERSION}. Please install psql>=${MIN_REQUIRED_MAJOR_VERSION}\e[0m"
        psql_wrong_version=1
    fi
}

print_dependencies() {
    # Properly format the dependencies and print them. with minimum version required, exact version no version specification etc
    local dependencies=("$@")
    for dependency in "${dependencies[@]}"; do
        IFS='|' read -r package version_type required_version <<< "$dependency"
        case "$version_type" in
            min)
                if [ "$required_version" != "0" ]; then
                    echo "$package with minimum version $required_version"
                else 
                    echo "$package"
                fi
                ;;
            exact)
                echo "$package with exact version $required_version"
                ;;
        esac
    done
}

print_misc_dependencies(){
    echo ""
    echo -e "\e[33mBinutils:\e[0m"
    echo "Minimum version: 2.25"
    echo ""
    echo -e "\e[33mJava:\e[0m"
    echo "Minimum version: 17"
    echo ""
    echo -e "\e[33mpg_dump:\e[0m"
    echo "Minimum version: 14"
    echo ""
    echo -e "\e[33mpg_restore:\e[0m"
    echo "Minimum version: 14"
    echo ""
    echo -e "\e[33mpsql:\e[0m"
    echo "Minimum version: 14"
}

#=============================================================================
# CENTOS/RHEL
#=============================================================================

centos_main() {
    # If --help is passed, print dependencies and exit.
    if [ "$PRINT_DEPENDENCIES" = "true" ]; then
        echo ""
        echo -e "\e[33mCentOS/RHEL dependencies:\e[0m"
        print_misc_dependencies
        echo ""
        echo -e "\e[33mYum packages:\e[0m"
        print_dependencies "${centos_yum_package_requirements[@]}"
        print_steps_to_install_oic_on_centos
        exit 0
    fi

    # If --force-install is not passed, check dependencies.
    if [ "$FORCE_INSTALL" = "false" ]; then
        echo "Checking dependencies..."
        check_binutils_version
        check_java
        check_pg_dump_and_pg_restore_version
        check_yum_dependencies
        # If either of the yum or cpan dependencies are missing or binutils wrong version, exit with error.
        if { [ ${#centos_missing_yum_packages[@]} -ne 0 ] || [ ${#missing_cpan_modules[@]} -ne 0 ] || [ "$binutils_wrong_version" -eq 1 ] || [ "$java_wrong_version" -eq 1 ] || [ "$pg_dump_wrong_version" -eq 1 ] || [ "$pg_restore_wrong_version" -eq 1 ] || [ "$psql_wrong_version" -eq 1 ]; } && [ "$FORCE_INSTALL" = "false" ]; then 
            echo ""
            echo -e "\e[33mThe script searches for specific package names only. If similar packages are not detected but are present and deemed reliable, use --force-install to install Voyager.\e[0m"
            exit 1
        fi

        if [ "$CHECK_ONLY_DEPENDENCIES" = "true" ]; then
            echo ""
            echo "All dependencies are satisfied."
            exit 0
        fi
    else
        echo ""
        echo -e "\e[33mForce install option is passed. Skipping dependency checks.\e[0m"
    fi

    # Prompt the user for permission to install the packages
    while true; do
        echo ""
	    echo -n "Do you want to proceed with the installation of packages (cpan modules, ora2pg, debezium, yb-voyager)? (y/n):"
	    read yn
	    case $yn in
		[Yy]* )
			break;;
		[Nn]* )
			echo "Installation cancelled."
            exit 0;;
		* ) ;;
	    esac
	done

    echo ""
    echo "Installing cpan modules..."
    echo ""
    for module_info in "${cpan_modules_requirements[@]}"; do
        # Split each entry by '|' to get module details
        IFS="|" read -r module_name requirement_type required_version package <<< "$module_info"
    
        # Call the install function with module details
        install_perl_module "$module_name" "$requirement_type" "$required_version" "$package"
    done
    sudo yum install -y -q mariadb-connector-c*.rpm 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: mariadb-connector-c did not get installed.\e[0m"
        exit 1
    fi
    sudo yum install -y -q perl-DBD-MySQL*.rpm 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: perl-DBD-MySQL did not get installed.\e[0m"
        exit 1
    fi
    sudo yum install -y -q perl-DBD-Oracle*.rpm 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: perl-DBD-Oracle did not get installed.\e[0m"
        exit 1
    fi

    echo "Installing ora2pg..."
    sudo yum install -y -q ora2pg*.noarch.rpm 1>&2 
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: ora2pg did not get installed.\e[0m"
        exit 1
    fi
    echo ""
    # The package name is like debezium-2.3.3-1.8.0b0111.noarch.rpm. The DEBEZIUM_YUM_VERSION is 2.3.3-1.8.0. Add * to match the version.
    echo "Installing debezium..."
    sudo yum install -y -q debezium*.noarch.rpm 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: debezium did not get installed.\e[0m"
        exit 1
    fi
    echo ""
    echo "Installing yb-voyager..."
    # The package name is like yb-voyager-1.8.0-b0111.x86_64.rpm. The YB_VOYAGER_YUM_VERSION is 1.8.0. Add * to match the version.
    sudo yum install -y -q yb-voyager*.x86_64.rpm 1>&2
  
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: yb-voyager did not get installed.\e[0m"
        exit 1
    fi
    echo ""
    echo "Installation completed." 

    set +x 
}

print_steps_to_install_oic_on_centos() {
    echo ""
    echo -e "\e[33mOracle Instant Client installation help for Centos/RHEL:\e[0m"
    echo ""
    echo "You can download the oracle instant client rpms from the following links:"
    echo ""
    echo -e "\e[33moracle-instantclient-tools:\e[0m"
    echo "https://download.oracle.com/otn_software/linux/instantclient/215000/oracle-instantclient-tools-21.5.0.0.0-1.x86_64.rpm"
    echo ""
    echo -e "\e[33moracle-instantclient-basic:\e[0m"
    echo "https://download.oracle.com/otn_software/linux/instantclient/215000/oracle-instantclient-basic-21.5.0.0.0-1.x86_64.rpm"
    echo ""
    echo -e "\e[33moracle-instantclient-devel:\e[0m"
    echo "https://download.oracle.com/otn_software/linux/instantclient/215000/oracle-instantclient-devel-21.5.0.0.0-1.x86_64.rpm"
    echo ""
    echo -e "\e[33moracle-instantclient-jdbc:\e[0m"
    echo "https://download.oracle.com/otn_software/linux/instantclient/215000/oracle-instantclient-jdbc-21.5.0.0.0-1.x86_64.rpm"
    echo ""
    echo -e "\e[33moracle-instantclient-sqlplus:\e[0m"
    echo "https://download.oracle.com/otn_software/linux/instantclient/215000/oracle-instantclient-sqlplus-21.5.0.0.0-1.x86_64.rpm"
}

check_yum_package_version() {
    local package=$1
    local version_type=$2
    local required_version=$3
    local installed_version=$(yum list installed "$package" 2>/dev/null | awk '/^Installed Packages/ {getline; print $2}' | cut -d- -f1)

    if [ -z "$installed_version" ]; then
        case $version_type in
            min)
                if [ "$required_version" != "0" ]; then
                    centos_missing_yum_packages+=("$package with minimum version $required_version is not installed.")
                else 
                    centos_missing_yum_packages+=("$package is not installed.")
                fi
                ;;
            exact)
                centos_missing_yum_packages+=("$package with exact version $required_version is not installed.")
                ;;
        esac 
    else
        case "$version_type" in
        min)
            if [ "$required_version" != "0" ] && [ "$(printf '%s\n' "$required_version" "$installed_version" | sort -V | head -n1)" != "$required_version" ]; then
                centos_missing_yum_packages+=("$package version is less than $required_version. Found version: $installed_version")
            fi
        ;;
        exact)
            # If package name starts with oracle-instantclient then convert installed version from example 21.5.0.0.0-1 to 21.5.0.0.0
            if [[ "$package" =~ ^oracle-instantclient ]]; then
                installed_version=$(echo "$installed_version" | cut -d'-' -f1)
            fi
            if [ "$installed_version" != "$required_version" ]; then
                centos_missing_yum_packages+=("$package version is not $required_version. Found version: $installed_version")
            fi
        ;;
        esac
    fi
}

check_yum_dependencies() {
    for requirement in "${centos_yum_package_requirements[@]}"; do
        IFS='|' read -r package version_type required_version <<< "$requirement"
        check_yum_package_version "$package" "$version_type" "$required_version"
    done

    if [ ${#centos_missing_yum_packages[@]} -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: the following yum packages are missing or do not meet the version requirements:\e[0m"
        for missing in "${centos_missing_yum_packages[@]}"; do
            echo "$missing"
        done
    else
        echo ""
        echo "All yum dependencies are installed and meet the version requirements."
    fi
}

#=============================================================================
# UBUNTU
#=============================================================================

ubuntu_main() {
     # If --help is passed, print dependencies and exit.
    if [ "$PRINT_DEPENDENCIES" = "true" ]; then
        echo ""
        echo -e "\e[33mUbuntu dependencies:\e[0m"
        print_misc_dependencies
        echo ""
        echo -e "\e[33mApt packages:\e[0m"
        print_dependencies "${ubuntu_apt_package_requirements[@]}"
        print_steps_to_install_oic_on_ubuntu
        exit 0
    fi

    # If --force-install is not passed, check dependencies.
    if [ "$FORCE_INSTALL" = "false" ]; then
        echo "Checking dependencies..."
        check_binutils_version
        check_java
        check_pg_dump_and_pg_restore_version
        check_apt_dependencies

        # If either of the apt or cpan dependencies are missing or binutils wrong version, exit with error.
        if { [ ${#ubuntu_missing_apt_packages[@]} -ne 0 ] || [ ${#missing_cpan_modules[@]} -ne 0 ] || [ "$binutils_wrong_version" -eq 1 ] || [ "$java_wrong_version" -eq 1 ] || [ "$pg_dump_wrong_version" -eq 1 ] || [ "$pg_restore_wrong_version" -eq 1 ] || [ "$psql_wrong_version" -eq 1 ]; } && [ "$FORCE_INSTALL" = "false" ]; then
            echo ""
            echo -e "\e[33mThe script searches for specific package names only. If similar packages are not detected but are present and deemed reliable, use --force-install to install Voyager.\e[0m"
            exit 1
        fi
    
        if [ "$CHECK_ONLY_DEPENDENCIES" = "true" ]; then
            echo ""
            echo "All dependencies are satisfied."
            exit 0
        fi
    else 
        echo ""
        echo -e "\e[33mForce install option is passed. Skipping dependency checks.\e[0m"
    fi

    # Prompt the user for permission to install the packages
    while true; do
        echo ""
	    echo -n "Do you want to proceed with the installation of packages (cpan modules, ora2pg, debezium, yb-voyager)? (y/n):"
	    read yn
	    case $yn in
		[Yy]* )
			break;;
		[Nn]* )
			echo "Installation cancelled."
            exit 0;;
		* ) ;;
	    esac
	done

    echo ""
    echo "Installing cpan modules..."
    echo ""
    for module_info in "${cpan_modules_requirements[@]}"; do
        # Split each entry by '|' to get module details
        IFS="|" read -r module_name requirement_type required_version package <<< "$module_info"
    
        # Call the install function with module details
        install_perl_module "$module_name" "$requirement_type" "$required_version" "$package"
    done
    sudo dpkg -i ./libdbd-mysql-perl*.deb 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: libdbd-mysql-perl did not get installed.\e[0m"
        exit 1
    fi
    sudo dpkg -i ./libdbd-oracle-perl*.deb 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: libdbd-oracle-perl did not get installed.\e[0m"
        exit 1
    fi


    echo "Installing ora2pg..."
    sudo apt install -y -q ./ora2pg*all.deb 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: ora2pg did not get installed.\e[0m"
        exit 1
    fi
    echo ""
    echo "Installing debezium..."
    sudo apt install -y -q ./debezium*all.deb 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: debezium did not get installed.\e[0m"
        exit 1
    fi
    echo ""
    echo "Installing yb-voyager..."
    sudo apt install -y -q ./yb-voyager*amd64.deb 1>&2
    if [ $? -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: yb-voyager did not get installed.\e[0m"
        exit 1
    fi
    echo ""
    echo "Installation completed."

    set +x
}

print_steps_to_install_oic_on_ubuntu() {
    echo ""
    echo -e "\e[33mOracle Instant Client installation help for Ubuntu:\e[0m"
    echo ""
    echo "You can download the oracle instant client debs from the following links:"
    echo ""
    echo -e "\e[33moracle-instantclient-tools:\e[0m"
    echo "https://s3.us-west-2.amazonaws.com/downloads.yugabyte.com/repos/apt/pool/main/oracle-instantclient-tools_21.5.0.0.0-1_amd64.deb"
    echo ""
    echo -e "\e[33moracle-instantclient-basic:\e[0m"
    echo "https://s3.us-west-2.amazonaws.com/downloads.yugabyte.com/repos/apt/pool/main/oracle-instantclient-basic_21.5.0.0.0-1_amd64.deb"
    echo ""
    echo -e "\e[33moracle-instantclient-devel:\e[0m"
    echo "https://s3.us-west-2.amazonaws.com/downloads.yugabyte.com/repos/apt/pool/main/oracle-instantclient-devel_21.5.0.0.0-1_amd64.deb"
    echo ""
    echo -e "\e[33moracle-instantclient-jdbc:\e[0m"
    echo "https://s3.us-west-2.amazonaws.com/downloads.yugabyte.com/repos/apt/pool/main/oracle-instantclient-jdbc_21.5.0.0.0-1_amd64.deb"
    echo ""
    echo -e "\e[33moracle-instantclient-sqlplus:\e[0m"
    echo "https://s3.us-west-2.amazonaws.com/downloads.yugabyte.com/repos/apt/pool/main/oracle-instantclient-sqlplus_21.5.0.0.0-1_amd64.deb"
    echo ""
    echo -e "\e[33moracle-instantclient12.1-basic:\e[0m"
    echo "https://s3.us-west-2.amazonaws.com/downloads.yugabyte.com/repos/apt/pool/main/oracle-instantclient12.1-basic_12.1.0.2.0-1_amd64.deb"
}

check_apt_dependencies() {
    for requirement in "${ubuntu_apt_package_requirements[@]}"; do
        IFS='|' read -r package version_type required_version <<< "$requirement"
        check_apt_package_version "$package" "$version_type" "$required_version"
    done

    if [ ${#ubuntu_missing_apt_packages[@]} -ne 0 ]; then
        echo ""
        echo -e "\e[31mERROR: the following apt packages are missing or do not meet the version requirements:\e[0m"
        for missing in "${ubuntu_missing_apt_packages[@]}"; do
            echo "$missing"
        done
    else
        echo ""
        echo "All apt dependencies are installed and meet the version requirements."
    fi
}

check_apt_package_version() {
    local package=$1
    local version_type=$2
    local required_version=$3
    local installed_version=$(dpkg-query -f '${Version}' -W "$package" 2>/dev/null)

    if [ -z "$installed_version" ]; then
        case "$version_type" in
            min)
                if [ "$required_version" != "0" ]; then
                    ubuntu_missing_apt_packages+=("$package with minimum version $required_version is not installed.")
                else 
                    ubuntu_missing_apt_packages+=("$package is not installed.")
                fi
                ;;
            exact)
                ubuntu_missing_apt_packages+=("$package with exact version $required_version is not installed.")
                ;;
        esac
    else
        case "$version_type" in
        min)
            if [ "$required_version" != "0" ] && [ "$(printf '%s\n' "$required_version" "$installed_version" | sort -V | head -n1)" != "$required_version" ]; then
                ubuntu_missing_apt_packages+=("$package version is less than $required_version. Found version: $installed_version")
            fi
        ;;
        exact)
            # If package name starts with oracle-instantclient then convert installed version from example 21.5.0.0.0-1 to 21.5.0.0.0
            if [[ "$package" =~ ^oracle-instantclient ]]; then
                installed_version=$(echo "$installed_version" | cut -d'-' -f1)
            fi
            if [ "$installed_version" != "$required_version" ]; then
                ubuntu_missing_apt_packages+=("$package version is not $required_version. Found version: $installed_version")
            fi
        ;;
        esac
    fi
}

main 2>> $LOG_FILE
{
  set +x; set -e 
} 2> /dev/null


