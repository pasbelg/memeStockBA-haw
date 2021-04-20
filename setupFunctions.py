# Funktion, die das als Parameter übergebes Secret aus der Datei secrets raussucht und wiedergibt
def getSecret(systemName, secretName):
    secretName = secretName.lower()
    try:
        f = open('secrets', 'r')
    except:
        exit('Datei "secrets" wurde nicht gefunden. Bitte lege eine Datei mit dem namen secrets im Hauptverzeichnis ab')
    lines = f.read().splitlines()
    systems = [i for i, s in enumerate(lines) if s.startswith('#')]
    systemFound = False
    secretFound = False
    for index in systems:
        if systemName in lines[index]:
            systemFound = True
            availableSecrets = 0
            for secret in lines[index+1:]:
                if '#' in secret:
                    break
                availableSecrets += 1
                if secretName in secret.lower():
                    return secret.split(':')[1].replace(' ', '')
            if systemFound == True and secretFound == False:
                exit(
                    'Secret "' + secretName + '" ist unter ' + systemName + ' nicht zu finden.'+
                    'Folgende Secrets sind vorhanden: '+str([x.split(':')[0].replace(' ', '') for x in lines[index+1:index+availableSecrets] if x != ''])
                    )  
    if systemFound == False:
        exit('System "' + systemName + '" konnten nicht gefunden werden. Bitte überprüfe die Eingabe. Folgende System sind Vorhanden: ' + 
                str([(lines[x])[1:] for x in systems])
            )


