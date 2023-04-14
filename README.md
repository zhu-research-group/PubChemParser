

Use docker to download PubChem Files

docker run -it --volume C:\Users\russod\Data\PUBCHEM\Concise\Data:/root ubuntu

 wget -r -I /pubchem/Bioassay/Concise/  ftp://ftp.ncbi.nlm.nih.gov/pubchem/Bioassay/Concise/CSV
 
wget -r -I /pubchem/Compound/CURRENT-Full/  ftp://ftp.ncbi.nlm.nih.gov/pubchem/Compound/CURRENT-Full/SDF

unzip like:
find . -name "*.zip" | xargs -P 5 -I fileName sh -c 'unzip -o -d "$(dirname "fileName")/$(basename -s .zip "fileName")" "fileName"'
