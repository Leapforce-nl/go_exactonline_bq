package exactonline_bq

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/storage"

	errortools "github.com/leapforce-libraries/go_errortools"
	financial "github.com/leapforce-libraries/go_exactonline_new/financial"
	types "github.com/leapforce-libraries/go_types"
)

type ReportingBalance struct {
	OrganisationID_            int64
	SoftwareClientLicenceID_   int64
	SoftwareClientLicenseGuid_ string
	Created_                   time.Time
	Modified_                  time.Time
	ID                         string
	Amount                     float64
	AmountCredit               float64
	AmountDebit                float64
	BalanceType                string
	CostCenterCode             string
	CostCenterDescription      string
	CostUnitCode               string
	CostUnitDescription        string
	Count                      int32
	Division                   int32
	GLAccount                  string
	GLAccountCode              string
	GLAccountDescription       string
	ReportingPeriod            int32
	ReportingYear              int32
	Status                     int32
	Type                       int32
}

func getReportingBalance(c *financial.ReportingBalance, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string) ReportingBalance {
	t := time.Now()

	return ReportingBalance{
		organisationID,
		softwareClientLicenceID,
		softwareClientLicenseGuid,
		t, t,
		c.ID,
		c.Amount,
		c.AmountCredit,
		c.AmountDebit,
		c.BalanceType,
		c.CostCenterCode,
		c.CostCenterDescription,
		c.CostUnitCode,
		c.CostUnitDescription,
		c.Count,
		c.Division,
		c.GLAccount.String(),
		c.GLAccountCode,
		c.GLAccountDescription,
		c.ReportingPeriod,
		c.ReportingYear,
		c.Status,
		c.Type,
	}
}

func (service *Service) WriteReportingBalances(bucketHandle *storage.BucketHandle, organisationID int64, softwareClientLicenceID int64, softwareClientLicenseGuid string, _ *time.Time) ([]*storage.ObjectHandle, int, interface{}, *errortools.Error) {
	if bucketHandle == nil {
		return nil, 0, nil, nil
	}

	objectHandles := []*storage.ObjectHandle{}
	var w *storage.Writer

	call := service.FinancialService().NewGetReportingBalancesCall()

	rowCount := 0
	batchRowCount := 0
	batchSize := 10000

	for {
		reportingBalances, e := call.Do()
		if e != nil {
			return nil, 0, nil, e
		}

		if reportingBalances == nil {
			break
		}

		if batchRowCount == 0 {
			guid := types.NewGuid()
			objectHandle := bucketHandle.Object((&guid).String())
			objectHandles = append(objectHandles, objectHandle)

			w = objectHandle.NewWriter(context.Background())
		}

		for _, tl := range *reportingBalances {
			batchRowCount++

			b, err := json.Marshal(getReportingBalance(&tl, organisationID, softwareClientLicenceID, softwareClientLicenseGuid))
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write data
			_, err = w.Write(b)
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}

			// Write NewLine
			_, err = fmt.Fprintf(w, "\n")
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
		}

		if batchRowCount > batchSize {
			// Close and flush data
			err := w.Close()
			if err != nil {
				return nil, 0, nil, errortools.ErrorMessage(err)
			}
			w = nil

			fmt.Printf("#ReportingBalances flushed: %v\n", batchRowCount)

			rowCount += batchRowCount
			batchRowCount = 0
		}
	}

	if w != nil {
		// Close and flush data
		err := w.Close()
		if err != nil {
			return nil, 0, nil, errortools.ErrorMessage(err)
		}

		rowCount += batchRowCount
	}

	fmt.Printf("#ReportingBalances: %v\n", rowCount)

	return objectHandles, rowCount, ReportingBalance{}, nil
}
