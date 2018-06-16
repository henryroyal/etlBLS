package main

import (
	"github.com/henryroyal/bls/series"
	"github.com/henryroyal/bls/series/employment"
	"github.com/henryroyal/bls/series/injuries"
	"github.com/henryroyal/bls/series/international"
	"github.com/henryroyal/bls/series/occupations"
	"github.com/henryroyal/bls/series/pay"
	"github.com/henryroyal/bls/series/prices"
	"github.com/henryroyal/bls/series/productivity"
	"github.com/henryroyal/bls/series/spending"
	"path"
	"os"
	"log"
	"net/http"
	"bufio"
	"io"
	"sync"
)

func main() {

	datasets := []series.DatasetDescriptor{
		// employment
		employment.NewBusinessEmploymentDynamics(),
		employment.NewNationalEmploymentHoursAndEarnings(),
		employment.NewNationalEmploymentHoursAndEarningsSIC(),
		//employment.NewStateAndCountyEmploymentAndWagesQuarterly(), // en is missing
		employment.NewGreenGoodsAndServices(),
		employment.NewGeographicProfile(),
		employment.NewJobOpeningsAndLaborTurnoverSIC(),
		employment.NewJobOpeningsAndLaborTurnover(),
		employment.NewLocalAreaUnemploymentStatistics(),
		employment.NewMassLayoffStatistics(),
		employment.NewOccupationalEmploymentStatistics(),
		employment.NewStateAndAreaEmploymentHoursAndEarningsSIC(),
		employment.NewStateAndAreaEmploymentHoursAndEarnings(),

		// injuries and illnesses
		injuries.NewNonfatalOccupationalInjuries1992to2001(),
		injuries.NewFatalOccupationalInjuries1992to2002(),
		injuries.NewNonfatalOccupationalInjuries2003to2010(),
		injuries.NewNonfatalOccupationalInjuriesPost2011(),
		injuries.NewFatalOccupationalInjuries2003to2010(),
		injuries.NewFatalOccupationalInjuriesPost2011(),
		injuries.NewNonfatalOccupationalInjuries2002(),
		injuries.NewNonfatalOccupationalInjuriesAndIllnessesPre1989(),
		injuries.NewNonfatalOccupationalInjuriesAndIllnesses2003to2013(),
		injuries.NewOccupationalInjuriesAndIllnessesPost2014(),
		injuries.NewNonfatalOccupationalInjuriesAndIllnesses1989to2001(),
		injuries.NewNonfatalOccupationalInjuriesAndIllnesses2002(),

		// international
		international.NewImportExportPriceIndexes(),

		// occupations
		occupations.NewOccupationalRequirementsSurvey(),

		// pay
		pay.NewEmployerCostForEmployeeCompensationSIC(),
		pay.NewEmployerCostForEmployeeCompensationQuarterly(),
		pay.NewEmployeeBenefitsSurvey(),
		pay.NewEmploymentCostIndexSIC(),
		pay.NewBenefitsCompensationSurvey(),
		pay.NewNationalCompensationSurvey(),
		pay.NewModeledWageEstimates(),
		pay.NewWorkStoppageData(),

		// prices
		prices.NewAveragePriceData(),
		prices.NewConsumerPriceIndexAllUrban(),
		prices.NewConsumerPriceIndexUrbanWageWorkers(),
		prices.NewDepartmentStoreInventoryPriceIndex(),
		prices.NewLegacyConsumerPriceIndexAllUrban(),
		prices.NewLegacyConsumerPriceIndexUrbanWageWorkers(),
		prices.NewLegacyProducerPriceIndexIndustryDataNAICS(),
		prices.NewProducerPriceIndexIndustryData(),
		prices.NewLegacyProducerPriceIndexIndustryDataSIC(),
		prices.NewChainedConsumerPriceIndexAllUrban(),
		prices.NewLegacyProducerPriceIndexCommodityData(),
		prices.NewProducerPriceIndexCommodityData(),

		// productivity
		productivity.NewIndustryProductivity(),
		productivity.NewMajorSectorMultifactorProductivity(),
		productivity.NewMajorSectorProductivityAndCosts(),

		// spending
		spending.NewConsumerExpenditureSurvey(),
		spending.NewAmericanTimeUseSurvey(),
	}

	etl(datasets)
}

func etl(datasets []series.DatasetDescriptor) {

	c := make(chan string)
	var wait sync.WaitGroup
	datasetCount := len(datasets)

	wait.Add(datasetCount)
	for _, dataset := range datasets {

		go func(dataset series.DatasetDescriptor, c chan string) {

			defer wait.Done()
			log.Println(dataset)

			files, err := dataset.DatasetFiles()
			if err != nil {
				log.Fatalln(err)
			}

			directory := path.Join("./", dataset.String())
			err = os.MkdirAll(directory, os.FileMode(0750))
			if err != nil {
				log.Fatalln(err)
			}

			for _, file := range files {
				if file == "" {
					// ignore top-level index
					continue
				}

				url := dataset.DatasetURL() + file
				log.Println("downloading:", url)
				resp, err := http.Get(url)
				if err != nil {
					log.Fatalln(err)
				}
				defer resp.Body.Close()

				outfilePath := path.Join(directory, file)
				fh, err := os.OpenFile(outfilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0640)
				if err != nil {
					log.Fatalln(err)
				}
				reader := bufio.NewReader(resp.Body)

				io.Copy(fh, reader)
			}
			c <- dataset.String()
		}(dataset, c)
	}

	for range datasets {
		log.Println("completed:", <-c)
	}
	wait.Wait()
	close(c)

}
